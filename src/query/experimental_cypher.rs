use std::collections::{BTreeMap, HashMap};
use std::sync::{Mutex, OnceLock};

use hydradb_cypher_engine::{
    EngineError, ListParameterMap, LogicalQuery, LowerTimings, LoweredTemplate, ParameterMap, PreparedQuery, ScalarValue, UnknownStatistics,
};

use crate::QueryFailureReason;
use crate::{GraphError, QueryColumn, Result, VertexPropertyValue};

pub(crate) struct ExperimentalCypherRequest {
    pub(crate) explain: bool,
    pub(crate) prepared: PreparedQuery,
}

pub(crate) fn prepare_experimental_cypher(
    query: &str,
    parameters: &BTreeMap<String, VertexPropertyValue>,
    lists: &BTreeMap<String, Vec<VertexPropertyValue>>,
) -> Result<ExperimentalCypherRequest> {
    let (explain, logical) = lower_experimental_cypher(query, parameters, lists)?;
    // Column discovery refuses at the same boundary the shard does. Otherwise a
    // client can be told a query has columns and then be refused when it runs
    // it, which is a worse answer than the refusal it should have had here.
    logical.capability().into_result().map_err(experimental_engine_error)?;
    let prepared = logical
        .plan_with_statistics(&UnknownStatistics)
        .map_err(experimental_engine_error)?;
    prepared.capability().into_result().map_err(experimental_engine_error)?;
    Ok(ExperimentalCypherRequest { explain, prepared })
}

/// Validate the complete production preparation boundary without executing it.
#[cfg(feature = "experimental-cypher-engine")]
pub fn validate_experimental_cypher_preparation(
    query: &str,
    parameters: &BTreeMap<String, VertexPropertyValue>,
    lists: &BTreeMap<String, Vec<VertexPropertyValue>>,
) -> Result<()> {
    prepare_experimental_cypher(query, parameters, lists).map(|_| ())
}

pub(crate) fn lower_experimental_cypher(
    query: &str,
    parameters: &BTreeMap<String, VertexPropertyValue>,
    lists: &BTreeMap<String, Vec<VertexPropertyValue>>,
) -> Result<(bool, LogicalQuery)> {
    let (explain, logical, _) = lower_experimental_cypher_cached(query, parameters, lists)?;
    Ok((explain, logical))
}

/// Like [`lower_experimental_cypher`], also reporting whether the parsed and
/// lowered template came from the process-wide cache.
pub(crate) fn lower_experimental_cypher_cached(
    query: &str,
    parameters: &BTreeMap<String, VertexPropertyValue>,
    lists: &BTreeMap<String, Vec<VertexPropertyValue>>,
) -> Result<(bool, LogicalQuery, bool)> {
    let (explain, logical, lowering) = lower_experimental_cypher_timed(query, parameters, lists, None)?;
    Ok((explain, logical, lowering.cache_hit))
}

/// How one request obtained its lowered template.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct TemplateLowering {
    pub(crate) cache_hit: bool,
    /// Parser and lowering time. Both zero on a cache hit, which did neither.
    pub(crate) timings: LowerTimings,
}

/// Like [`lower_experimental_cypher_cached`], also reporting the parse and
/// lower split of a cache miss.
/// `session_user` is what GQL's `SESSION_USER` reads; it is bound per
/// request, never into the cached template.
pub(crate) fn lower_experimental_cypher_timed(
    query: &str,
    parameters: &BTreeMap<String, VertexPropertyValue>,
    lists: &BTreeMap<String, Vec<VertexPropertyValue>>,
    session_user: Option<&str>,
) -> Result<(bool, LogicalQuery, TemplateLowering)> {
    lower_experimental_cypher_with_id_properties(query, parameters, lists, session_user, false)
}

pub(crate) fn lower_experimental_cypher_with_id_properties(
    query: &str,
    parameters: &BTreeMap<String, VertexPropertyValue>,
    lists: &BTreeMap<String, Vec<VertexPropertyValue>>,
    session_user: Option<&str>,
    id_properties: bool,
) -> Result<(bool, LogicalQuery, TemplateLowering)> {
    let (explain, query) = split_explain(query)?;
    let parameters = experimental_parameter_map(parameters)?;
    let lists = experimental_list_parameter_map(lists);
    let (template, lowering) = lowered_template_with_id_properties(query, id_properties)?;
    Ok((explain, template.bind_as(&parameters, &lists, session_user), lowering))
}

/// Upper bound on cached templates. Each holds one AST and one logical plan
/// for a distinct query text; a few thousand covers any real application's
/// prepared-statement vocabulary while bounding memory for an adversarial
/// stream of unique texts.
const LOWERED_TEMPLATE_CACHE_ENTRIES: usize = 4_096;
/// Upper bound on the estimated heap the cache retains across all entries.
/// A typical application query costs 2–6 KiB, so this holds thousands of
/// shapes; a stream of large texts is evicted by bytes long before it
/// reaches the entry cap.
const LOWERED_TEMPLATE_CACHE_MAX_BYTES: usize = 32 * 1024 * 1024;
/// Texts longer than this are lowered but never cached: a giant one-off
/// query must not evict the working set.
const LOWERED_TEMPLATE_CACHE_MAX_QUERY_BYTES: usize = 16 * 1024;

/// Process-wide cache of parsed-and-lowered query templates, keyed by query
/// text after the `EXPLAIN` prefix is stripped.
///
/// Parsing is the single largest fixed cost of an experimental request
/// (75–120 µs for a shape with `ORDER BY`, against ~50 µs for the whole
/// legacy request), and the same text is parsed up to three times per
/// request across Bolt column discovery, client preparation and shard
/// execution. Templates depend on nothing but the text — parameters are bound
/// afterwards and statistics are consulted only at physical planning — so the
/// cache needs no invalidation short of a new binary.
///
/// Process-wide rather than per shard for the same reason: the template is a
/// pure function of the text, and the client transports that also lower have
/// no shard in hand.
#[derive(Clone)]
struct CachedLoweringFailure {
    reason: QueryFailureReason,
    message: String,
}
type CachedLowering = std::result::Result<LoweredTemplate, CachedLoweringFailure>;

struct LoweredTemplateEntry {
    template: CachedLowering,
    bytes: usize,
    last_used: u64,
}

/// A byte- and entry-bounded LRU. `entries` answers lookups; `by_use` orders
/// keys by their last-use tick so eviction is a pop of the smallest tick and
/// a hit is two O(log n) index updates. The tick is a monotonic counter, so
/// it is unique per touch and `by_use` never needs to resolve collisions.
/// Nothing here is O(n): a sustained stream of distinct texts costs each
/// request one parse plus a handful of map operations under the lock.
struct LoweredTemplateCache {
    entries: HashMap<String, LoweredTemplateEntry>,
    by_use: BTreeMap<u64, String>,
    max_entries: usize,
    max_bytes: usize,
    resident_bytes: usize,
    clock: u64,
}

impl LoweredTemplateCache {
    fn new(max_entries: usize, max_bytes: usize) -> Self {
        Self {
            entries: HashMap::new(),
            by_use: BTreeMap::new(),
            max_entries,
            max_bytes,
            resident_bytes: 0,
            clock: 0,
        }
    }

    fn tick(&mut self) -> u64 {
        self.clock += 1;
        self.clock
    }

    fn get(&mut self, query: &str) -> Option<CachedLowering> {
        let now = self.tick();
        let entry = self.entries.get_mut(query)?;
        self.by_use.remove(&entry.last_used);
        entry.last_used = now;
        self.by_use.insert(now, query.to_string());
        Some(entry.template.clone())
    }

    fn remove_entry(&mut self, query: &str) -> Option<LoweredTemplateEntry> {
        let entry = self.entries.remove(query)?;
        self.by_use.remove(&entry.last_used);
        self.resident_bytes = self.resident_bytes.saturating_sub(entry.bytes);
        Some(entry)
    }

    fn evict_least_recently_used(&mut self) -> bool {
        let Some((_, victim)) = self.by_use.pop_first() else {
            return false;
        };
        if let Some(entry) = self.entries.remove(&victim) {
            self.resident_bytes = self.resident_bytes.saturating_sub(entry.bytes);
        }
        true
    }

    fn insert(&mut self, query: &str, template: LoweredTemplate) {
        self.insert_outcome(query, Ok(template));
    }

    fn insert_outcome(&mut self, query: &str, template: CachedLowering) {
        let bytes = query.len()
            + match &template {
                Ok(template) => template.estimated_bytes(),
                Err(error) => error.message.len() + std::mem::size_of::<CachedLoweringFailure>(),
            };
        if bytes > self.max_bytes {
            return;
        }
        self.remove_entry(query);
        // Evict least recently used until both the entry and byte budgets
        // hold. Each eviction is one pop from the ordered index.
        while !self.entries.is_empty() && (self.entries.len() >= self.max_entries || self.resident_bytes + bytes > self.max_bytes) {
            if !self.evict_least_recently_used() {
                break;
            }
        }
        let now = self.tick();
        self.resident_bytes += bytes;
        self.by_use.insert(now, query.to_string());
        self.entries.insert(
            query.to_string(),
            LoweredTemplateEntry {
                template,
                bytes,
                last_used: now,
            },
        );
    }
}

static LOWERED_TEMPLATE_CACHE: OnceLock<Mutex<LoweredTemplateCache>> = OnceLock::new();

fn lowered_template_cache() -> &'static Mutex<LoweredTemplateCache> {
    LOWERED_TEMPLATE_CACHE.get_or_init(|| {
        Mutex::new(LoweredTemplateCache::new(
            LOWERED_TEMPLATE_CACHE_ENTRIES,
            LOWERED_TEMPLATE_CACHE_MAX_BYTES,
        ))
    })
}

fn lowered_template(query: &str) -> Result<(LoweredTemplate, TemplateLowering)> {
    lowered_template_with_id_properties(query, false)
}

fn lowered_template_with_id_properties(query: &str, id_properties: bool) -> Result<(LoweredTemplate, TemplateLowering)> {
    // A separate bounded cache prevents cross-layout reuse of the same text.
    static PROPERTY_CACHE: OnceLock<Mutex<LoweredTemplateCache>> = OnceLock::new();
    let cache = if id_properties {
        PROPERTY_CACHE.get_or_init(|| {
            Mutex::new(LoweredTemplateCache::new(
                LOWERED_TEMPLATE_CACHE_ENTRIES,
                LOWERED_TEMPLATE_CACHE_MAX_BYTES,
            ))
        })
    } else {
        lowered_template_cache()
    };
    let cacheable = query.len() <= LOWERED_TEMPLATE_CACHE_MAX_QUERY_BYTES;
    if cacheable {
        let cached = cache.lock().unwrap_or_else(|poisoned| poisoned.into_inner()).get(query);
        if let Some(template) = cached {
            return Ok((
                template.map_err(|error| experimental_query_error(error.reason, error.message))?,
                TemplateLowering {
                    cache_hit: true,
                    timings: LowerTimings::default(),
                },
            ));
        }
    }
    let (template, timings) = match LoweredTemplate::lower_timed_with_id_properties(query, id_properties) {
        Ok(lowered) => lowered,
        Err(error) => {
            // Lowering depends only on query text, never parameters or graph
            // statistics. Cache deterministic rejections under the same LRU
            // entry and byte limits so routing layers do not parse them again.
            if cacheable {
                if let Some(reason) = error.failure_reason() {
                    cache.lock().unwrap_or_else(|p| p.into_inner()).insert_outcome(
                        query,
                        Err(CachedLoweringFailure {
                            reason,
                            message: error.to_string(),
                        }),
                    );
                }
            }
            return Err(experimental_engine_error(error));
        }
    };
    if cacheable {
        cache
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(query, template.clone());
    }
    Ok((template, TemplateLowering { cache_hit: false, timings }))
}

pub(crate) fn experimental_query_columns(
    query: &str,
    parameters: &BTreeMap<String, VertexPropertyValue>,
    lists: &BTreeMap<String, Vec<VertexPropertyValue>>,
) -> Result<Vec<QueryColumn>> {
    let request = prepare_experimental_cypher(query, parameters, lists)?;
    if request.explain {
        return Ok(vec![QueryColumn::new("plan")]);
    }
    Ok(request
        .prepared
        .column_names()
        .map_err(experimental_engine_error)?
        .into_iter()
        .map(QueryColumn::new)
        .collect())
}

/// Whether the Cypher 25 adapter lowers `query` to a typed read plan.
///
/// The fallback `classify_opencypher_query_access` asks when libcypher cannot
/// parse a query. Lowering goes through the template cache, so the request
/// that follows finds the template already built.
pub(crate) fn is_cypher25_read_query(query: &str) -> bool {
    split_explain(query).is_ok_and(|(_, query)| lowered_template(query).is_ok())
}

pub(crate) fn is_experimental_explain(query: &str) -> bool {
    query
        .split_whitespace()
        .next()
        .is_some_and(|keyword| keyword.eq_ignore_ascii_case("EXPLAIN"))
}

pub(crate) fn experimental_parameter_map(parameters: &BTreeMap<String, VertexPropertyValue>) -> Result<ParameterMap> {
    parameters
        .iter()
        .map(|(name, value)| Ok((name.clone(), property_to_scalar(value))))
        .collect()
}

/// The list sidecar the engine's `IN` reads. Element conversion is the same
/// one scalars go through, so a list of strings binds exactly as fifty separate
/// string parameters would.
pub(crate) fn experimental_list_parameter_map(lists: &BTreeMap<String, Vec<VertexPropertyValue>>) -> ListParameterMap {
    lists
        .iter()
        .map(|(name, values)| (name.clone(), values.iter().map(property_to_scalar).collect::<Vec<_>>()))
        .collect()
}

pub(crate) fn property_to_scalar(value: &VertexPropertyValue) -> ScalarValue {
    match value {
        VertexPropertyValue::Integer(value) => ScalarValue::Integer(i128::from(*value)),
        VertexPropertyValue::SignedInteger(value) => ScalarValue::Integer(i128::from(*value)),
        VertexPropertyValue::Bool(value) => ScalarValue::Boolean(*value),
        VertexPropertyValue::Float(value) => ScalarValue::Float(value.0.to_string().into()),
        VertexPropertyValue::String(value) => ScalarValue::String(value.clone().into()),
        VertexPropertyValue::Date(value) => ScalarValue::Date(*value),
        VertexPropertyValue::List(values) => ScalarValue::List(match values {
            crate::VertexPropertyList::Empty => Vec::new(),
            crate::VertexPropertyList::Integer(values) => values
                .iter()
                .map(|value| match value {
                    crate::VertexPropertyInteger::Unsigned(value) => ScalarValue::Integer(i128::from(*value)),
                    crate::VertexPropertyInteger::Signed(value) => ScalarValue::Integer(i128::from(*value)),
                })
                .collect(),
            crate::VertexPropertyList::Bool(values) => values.iter().copied().map(ScalarValue::Boolean).collect(),
            crate::VertexPropertyList::Float(values) => values.iter().map(|value| ScalarValue::Float(value.0.to_string().into())).collect(),
            crate::VertexPropertyList::String(values) => values.iter().map(|value| ScalarValue::String(value.clone().into())).collect(),
            crate::VertexPropertyList::Date(values) => values.iter().copied().map(ScalarValue::Date).collect(),
        }),
    }
}

/// Move decoded property strings into the engine when the adapter owns the
/// record. This avoids copying large relationship metadata blobs on hydration.
pub(crate) fn property_into_scalar(value: VertexPropertyValue) -> ScalarValue {
    match value {
        VertexPropertyValue::String(value) => ScalarValue::String(value.into_boxed_str()),
        value => property_to_scalar(&value),
    }
}

pub(crate) fn experimental_engine_error(error: EngineError) -> GraphError {
    // A storage failure the adapter raised as a `GraphError` -- a timeout, a
    // scan/row/byte budget, admission, a corrupt record -- is that error, with
    // the class and client code legacy gives it. None of them is a gap in the
    // query language, and calling one so tells a Bolt client its syntax is
    // wrong.
    let error = match error.into_backend_error::<GraphError>() {
        Ok(storage) => return storage,
        Err(error) => error,
    };
    match error.failure_reason() {
        Some(reason) => GraphError::GqlQuery {
            dialect: "Cypher25",
            status: Box::new(gql_status_report(&error.status_object())),
            feature: error.to_string(),
            reason,
        },
        // A storage failure that carried no typed error: its real class is
        // lost, so it keeps the wording clients always saw and is left
        // uncounted.
        None => GraphError::UnclassifiedQuery {
            dialect: "Cypher25",
            feature: error.to_string(),
        },
    }
}

/// The engine's GQL-status object as the transports carry it.
pub(crate) fn gql_status_report(object: &hydradb_cypher_engine::GqlStatusObject) -> crate::GqlStatusReport {
    let mut diagnostic = std::collections::BTreeMap::new();
    if let Some(record) = &object.diagnostic {
        diagnostic.insert("OPERATION".to_string(), record.operation.to_string());
        diagnostic.insert("OPERATION_CODE".to_string(), record.operation_code.to_string());
        diagnostic.insert(
            "CURRENT_SCHEMA".to_string(),
            record.current_schema.clone().unwrap_or_else(|| "/".to_string()),
        );
        if let Some(reference) = &record.invalid_reference {
            diagnostic.insert("INVALID_REFERENCE".to_string(), reference.clone());
        }
        diagnostic.extend(record.additional.iter().cloned());
    }
    crate::GqlStatusReport {
        gql_status: object.status.to_string(),
        description: object.description.clone(),
        diagnostic,
    }
}

pub(crate) fn experimental_query_error(reason: QueryFailureReason, error: impl std::fmt::Display) -> GraphError {
    GraphError::UnsupportedQuery {
        dialect: "Cypher25",
        feature: error.to_string(),
        reason,
    }
}

fn split_explain(query: &str) -> Result<(bool, &str)> {
    let query = query.trim();
    let Some(first) = query.split_whitespace().next() else {
        return Err(experimental_query_error(QueryFailureReason::ParseError, "query is empty"));
    };
    if !is_experimental_explain(query) {
        return Ok((false, query));
    }
    let rest = query[first.len()..].trim_start();
    if rest.is_empty() {
        return Err(experimental_query_error(
            QueryFailureReason::ParseError,
            "EXPLAIN must be followed by a Cypher query",
        ));
    }
    Ok((true, rest))
}
