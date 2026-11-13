//! Subscriber assembly: the filter, the fmt layer, and the redaction that both
//! of them route through.
//!
//! # Why this crate formats JSON itself
//!
//! `tracing-subscriber` ships a perfectly good JSON formatter, and the first
//! design here used it. It cannot deliver either of the two things this crate
//! promises:
//!
//! 1. **A guaranteed `binary` field.** There is no hook for constant fields.
//!    The usual workaround — a process-wide root span — silently fails for
//!    every `tokio::spawn` that is not explicitly `.instrument()`ed, and the
//!    kernel spawns freely.
//! 2. **Guaranteed redaction.** `Format<Json>::format_event` serialises event
//!    fields through `tracing_serde` *directly*, bypassing `FormatFields`
//!    (`tracing-subscriber-0.3.23/src/fmt/format/json.rs:266`). A custom field
//!    formatter therefore reaches span fields only — event fields, which is
//!    where a stray `parameters = …` would actually land, go out unredacted.
//!
//! Since redaction is a safety property rather than a nicety, and a safety net
//! with a hole in it is worse than none because nobody notices it failed, the
//! JSON path implements [`FormatEvent`] here. Text mode keeps the built-in
//! formatter, which *does* route through `FormatFields` and so needs only the
//! field wrapper.

use std::fmt;

use tracing::{Event, Subscriber};
use tracing_subscriber::field::{RecordFields, VisitOutput};
use tracing_subscriber::fmt::format::{DefaultVisitor, Writer};
use tracing_subscriber::fmt::{FmtContext, FormatEvent, FormatFields, FormattedFields};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Layer};

use crate::config::{ServiceIdentity, TelemetryConfig};
use crate::redact::RedactingVisitor;
use crate::{TelemetryError, TelemetryGuard};

/// Install the global subscriber. See [`crate::init`].
pub fn install(config: TelemetryConfig) -> Result<TelemetryGuard, TelemetryError> {
    let filter = EnvFilter::try_new(&config.filter).map_err(|source| TelemetryError::Filter {
        directives: config.filter.clone(),
        source,
    })?;

    #[cfg(feature = "otlp")]
    {
        let (otlp_layers, providers) = crate::otlp::build(&config)?;
        // The empty case gets its own branch rather than attaching an empty
        // layer vector, and the difference is not stylistic: `Vec<L>`'s
        // `Layer::register_callsite` seeds `Interest::never()` and folds each
        // element into it, so an *empty* vector returns `never` — whereupon
        // `Layered::register_callsite` short-circuits and disables the callsite
        // for the whole subscriber. Attaching one silences every log line in
        // the process, fmt layer included.
        //
        // That is precisely the no-endpoint configuration: the default way to
        // run a node built with `--features otlp` but no collector, which the
        // plan requires to behave exactly like a normal build. The symptom is
        // also maximally confusing — the process boots, serves traffic and
        // prints nothing at all — so it stays a branch, not an `Option` whose
        // correctness rests on a blanket impl elsewhere.
        if otlp_layers.is_empty() {
            tracing_subscriber::registry()
                .with(filter)
                .with(fmt_layer(&config))
                .try_init()
                .map_err(|_| TelemetryError::AlreadyInitialised)?;
        } else {
            tracing_subscriber::registry()
                .with(filter)
                .with(fmt_layer(&config))
                .with(otlp_layers)
                .try_init()
                .map_err(|_| TelemetryError::AlreadyInitialised)?;
        }
        Ok(TelemetryGuard { providers })
    }

    #[cfg(not(feature = "otlp"))]
    {
        tracing_subscriber::registry()
            .with(filter)
            .with(fmt_layer(&config))
            .try_init()
            .map_err(|_| TelemetryError::AlreadyInitialised)?;
        Ok(TelemetryGuard { _private: () })
    }
}

/// The stdout layer, JSON or text according to [`TelemetryConfig::json`].
fn fmt_layer<S>(config: &TelemetryConfig) -> Box<dyn Layer<S> + Send + Sync>
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    if config.json {
        Box::new(
            tracing_subscriber::fmt::layer()
                .fmt_fields(RedactingFields::json())
                .event_format(HydraDBJson::new(config)),
        )
    } else {
        Box::new(
            tracing_subscriber::fmt::layer()
                .fmt_fields(RedactingFields::text())
                .with_target(true),
        )
    }
}

/// A [`FormatFields`] that routes every field through [`RedactingVisitor`]
/// before the underlying visitor sees it.
///
/// Both variants delegate the actual serialisation to `tracing-subscriber`'s
/// own visitors — this type only interposes. Reimplementing JSON escaping here
/// would be a second, worse copy of a solved problem.
#[derive(Clone, Copy, Debug)]
pub struct RedactingFields {
    json: bool,
}

impl RedactingFields {
    /// Redacted JSON object members, matching `JsonFields`' output.
    pub fn json() -> Self {
        Self { json: true }
    }

    /// Redacted `key=value` pairs, matching `DefaultFields`' output.
    pub fn text() -> Self {
        Self { json: false }
    }
}

impl<'writer> FormatFields<'writer> for RedactingFields {
    fn format_fields<R: RecordFields>(&self, mut writer: Writer<'writer>, fields: R) -> fmt::Result {
        if self.json {
            let mut visitor = JsonFieldVisitor::default();
            let mut redacting = RedactingVisitor::new(&mut visitor);
            fields.record(&mut redacting);
            let rendered = serde_json::to_string(&visitor.into_map()).map_err(|_| fmt::Error)?;
            // `FormattedFields` for a span stores the whole object; the event
            // formatter below splices it back in as raw JSON.
            write!(writer, "{rendered}")
        } else {
            let mut visitor = DefaultVisitor::new(writer, true);
            let mut redacting = RedactingVisitor::new(&mut visitor);
            fields.record(&mut redacting);
            visitor.finish()
        }
    }

    /// Merge later-recorded fields into the object already stored for a span.
    ///
    /// The default implementation *appends*: it pushes a space and calls
    /// [`FormatFields::format_fields`] again. For `key=value` text that is
    /// exactly right, and the text arm below keeps it. For JSON it is fatal —
    /// the stored buffer becomes `{"a":1} {"b":2}`, which is two objects and
    /// not a document, so the `serde_json::from_str` in
    /// [`HydraDBJson::format_event`] fails and drops the span's fields
    /// *entirely*. The span still appears in the `spans` array, with nothing
    /// but its name.
    ///
    /// That is not an edge case here, it is the common path. Attributes only
    /// known once the work is done — `error.class`, `hydradb.writer.epoch`,
    /// the three `last_promoted_*` fields — are declared
    /// `tracing::field::Empty` at span creation and filled with
    /// `Span::record`, which is what makes a healthy request pay nothing for
    /// them. Every one of those spans was losing its creation-time fields in
    /// the JSON logs: `hydradb.cell_id` and `hydradb.node_id` vanished from
    /// precisely the fence spans whose whole purpose is to be grouped by
    /// `cell_id`.
    ///
    /// OTLP was never affected — `tracing-opentelemetry` handles `on_record`
    /// itself — so this silently degraded the stdout logs alone, which is the
    /// half somebody reads at 2am with `grep`.
    fn add_fields(&self, current: &'writer mut FormattedFields<Self>, fields: &tracing::span::Record<'_>) -> fmt::Result {
        if !self.json {
            if !current.fields.is_empty() {
                current.fields.push(' ');
            }
            return self.format_fields(current.as_writer(), fields);
        }

        let mut merged: serde_json::Map<String, serde_json::Value> = if current.fields.is_empty() {
            serde_json::Map::new()
        } else {
            serde_json::from_str(&current.fields).map_err(|_| fmt::Error)?
        };

        let mut visitor = JsonFieldVisitor::default();
        let mut redacting = RedactingVisitor::new(&mut visitor);
        fields.record(&mut redacting);
        // Later wins. A field recorded twice is a deliberate correction — a
        // retry count climbing, an outcome resolving from `Empty` — so the
        // newest value is the true one.
        merged.extend(visitor.into_map());

        current.fields = serde_json::to_string(&merged).map_err(|_| fmt::Error)?;
        Ok(())
    }
}

/// Collects visited fields into a `serde_json` map.
#[derive(Default)]
struct JsonFieldVisitor {
    map: serde_json::Map<String, serde_json::Value>,
}

impl JsonFieldVisitor {
    fn into_map(self) -> serde_json::Map<String, serde_json::Value> {
        self.map
    }

    fn insert(&mut self, field: &tracing::field::Field, value: serde_json::Value) {
        self.map.insert(field.name().to_string(), value);
    }
}

impl tracing::field::Visit for JsonFieldVisitor {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn fmt::Debug) {
        self.insert(field, serde_json::Value::String(format!("{value:?}")));
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        self.insert(field, serde_json::Value::String(value.to_string()));
    }

    fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
        self.insert(field, serde_json::Value::from(value));
    }

    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        self.insert(field, serde_json::Value::from(value));
    }

    fn record_i128(&mut self, field: &tracing::field::Field, value: i128) {
        // i128 has no JSON number representation that survives every consumer;
        // a string is lossless where a truncating cast is not.
        self.insert(field, serde_json::Value::String(value.to_string()));
    }

    fn record_u128(&mut self, field: &tracing::field::Field, value: u128) {
        self.insert(field, serde_json::Value::String(value.to_string()));
    }

    fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
        self.insert(field, serde_json::Value::Bool(value));
    }

    fn record_f64(&mut self, field: &tracing::field::Field, value: f64) {
        match serde_json::Number::from_f64(value) {
            Some(number) => self.insert(field, serde_json::Value::Number(number)),
            // NaN and the infinities have no JSON encoding. Emitting the
            // Rust spelling beats dropping the field silently.
            None => self.insert(field, serde_json::Value::String(value.to_string())),
        }
    }

    fn record_error(&mut self, field: &tracing::field::Field, value: &(dyn std::error::Error + 'static)) {
        self.insert(field, serde_json::Value::String(value.to_string()));
    }
}

/// Span and event fields lifted to the top level of the line, and the key each
/// takes there.
///
/// The log collector maps a line's **root** keys onto warehouse columns by
/// name, and the log warehouse table has a column waiting for each entry below.
/// Nothing in that pipeline lifts a nested key: a value under `fields`, or
/// inside the `spans` array below, is bucketed whole into an opaque attributes
/// blob and the column stays null. So these are hoisted out of the enclosing
/// span — or off the event itself — onto every line emitted while it is open.
///
/// Which is the point for tenancy, and not merely a plumbing convenience.
/// Tenancy is a property of the *request*, not of the one line that happened to
/// mention it, so it is recorded once where the request is understood — see
/// `client_root_span` in the kernel — and every planner warning, admission
/// refusal and error logged underneath inherits it without knowing it exists.
///
/// The rest are plain renames from the name a Rust call site writes to the name
/// the warehouse column has. Left nested and spelled the `hydradb.*` way,
/// `error_type`, `correlation_id` and `operation` never populated their
/// columns at all. A rename table is all that gap ever was.
///
/// Values are copied **verbatim**, with no type coercion: `duration_ms` and
/// `result_count` face numeric columns, and the collector coerces them there.
/// Coercing here as well would mean two places to disagree about what
/// `elapsed_ms = "n/a"` means, and the one that runs first would silently win.
///
/// The base64 spellings (`hydradb.tenant.scope_id`) are deliberately not
/// promoted. They have no column, they are what `hydradb.scope` already spells
/// out on the span, and a root key with no column is copied into the attributes
/// map on every single line.
const PROMOTED_SPAN_FIELDS: &[(&str, &str)] = &[
    ("hydradb.tenant_id", "tenant_id"),
    ("hydradb.sub_tenant_id", "sub_tenant_id"),
    ("error.class", "error_type"),
    ("error", "error_message"),
    ("hydradb.correlation_id", "correlation_id"),
    ("hydradb.outcome", "event"),
    ("elapsed_ms", "duration_ms"),
    ("hydradb.query.rows_returned", "result_count"),
];

/// Root keys the line owns, which a same-named event field must not overwrite.
///
/// Event fields are flattened to the root so the collector can see them, and
/// flattening is the moment a field called `level` or `target` stops being data
/// and starts being a lie about the line itself: `tracing::info!(target = …)`
/// would rewrite the Rust module path the line was emitted from, and no reader
/// or query could tell the forged value from the real one. Colliding fields go
/// under `fields` instead — nested, so invisible to the column mapping, which
/// is the correct outcome for a value that has no column of its own anyway.
///
/// `trace_id` and `span_id` are reserved unconditionally, even though they are
/// only ever *written* under `otlp`. Reserving them per-feature would make the
/// shape of a line depend on how the binary was compiled, so a field named
/// `trace_id` would flatten to the root in a default build and nest in an OTLP
/// build — a difference that shows up as a column populated in one deployment
/// and null in the next.
const RESERVED_ROOT_KEYS: &[&str] = &[
    "timestamp",
    "level",
    "binary",
    "service_name",
    "instance",
    "version",
    "environment",
    "target",
    "trace_id",
    "span_id",
    "message",
    "operation",
    "span",
    "spans",
    // Not in the spec'd list, and deliberately here anyway: `fields` is the
    // object the colliding keys are *put into*, so flattening a field of that
    // name to the root would be clobbered by the very map built to protect it.
    "fields",
];

/// Whether `key` is a root key the line owns — either structural or a promoted
/// column.
fn is_reserved_root_key(key: &str) -> bool {
    RESERVED_ROOT_KEYS.contains(&key) || PROMOTED_SPAN_FIELDS.iter().any(|(_, column)| *column == key)
}

/// A field value rendered as message text.
///
/// A JSON string contributes its *contents*: `serde_json::Value::to_string`
/// would hand back `"\"connection refused\""`, quotes and escapes included, and
/// that is what a human reads in the `message` column. Everything else — a
/// number, a bool — has no unquoted spelling and keeps its JSON one.
fn text_of(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(text) => text.clone(),
        other => other.to_string(),
    }
}

/// The JSON event format.
///
/// Every line carries `binary` and `service_name`, which is what makes a shared
/// log sink separable — see the module docs for why the built-in formatter
/// cannot promise that.
///
/// The struct field stays `service`; only the emitted key is `service_name`,
/// which is the name of the log warehouse table's column and of the OTel resource
/// attribute every other service already writes. Spelling it `service` on the
/// wire bought a hydradb-only rename rule in the collector config, and a
/// collector rule that exists for exactly one producer is a rule that stops
/// being applied the day somebody rewrites the pipeline.
#[derive(Clone, Debug)]
pub struct HydraDBJson {
    binary: &'static str,
    service: &'static str,
    instance: String,
    version: String,
    environment: Option<String>,
}

impl HydraDBJson {
    /// Build from a config.
    pub fn new(config: &TelemetryConfig) -> Self {
        Self {
            binary: config.identity.binary(),
            service: config.identity.service_name(),
            instance: config.instance_id.clone(),
            version: config.service_version.clone(),
            environment: config.deployment_environment.clone(),
        }
    }

    /// The identity fields, for tests.
    pub fn identity(&self) -> ServiceIdentity {
        if self.binary == ServiceIdentity::GraphIndexer.binary() {
            ServiceIdentity::GraphIndexer
        } else {
            ServiceIdentity::GraphNode
        }
    }
}

impl<S, N> FormatEvent<S, N> for HydraDBJson
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
{
    fn format_event(&self, ctx: &FmtContext<'_, S, N>, mut writer: Writer<'_>, event: &Event<'_>) -> fmt::Result {
        let meta = event.metadata();
        let mut line = serde_json::Map::new();

        line.insert(
            "timestamp".to_string(),
            serde_json::Value::String(chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Micros, true)),
        );
        line.insert("level".to_string(), serde_json::Value::String(meta.level().to_string()));
        // Identity first-class on every line, not in a resource block the log
        // sink cannot see.
        line.insert("binary".to_string(), serde_json::Value::String(self.binary.to_string()));
        line.insert("service_name".to_string(), serde_json::Value::String(self.service.to_string()));
        line.insert("instance".to_string(), serde_json::Value::String(self.instance.clone()));
        line.insert("version".to_string(), serde_json::Value::String(self.version.clone()));
        if let Some(environment) = &self.environment {
            line.insert("environment".to_string(), serde_json::Value::String(environment.clone()));
        }
        line.insert("target".to_string(), serde_json::Value::String(meta.target().to_string()));

        // Trace correlation, top-level rather than under `fields`, because the
        // log collector reads these two keys by name to fill the OTLP
        // `logRecord`'s dedicated `traceId`/`spanId` — the fields a backend's
        // log-to-trace deep link resolves against. Absent, not zero, when no
        // trace is active: a zero id is a *valid-looking* id that resolves to
        // nothing, which is worse to debug than a missing key.
        #[cfg(feature = "otlp")]
        if let Some((trace_id, span_id)) = crate::bridge::current_trace_ids() {
            line.insert("trace_id".to_string(), serde_json::Value::String(trace_id));
            line.insert("span_id".to_string(), serde_json::Value::String(span_id));
        }

        // Event fields, redacted. This is the path the built-in JSON formatter
        // bypasses.
        let mut visitor = JsonFieldVisitor::default();
        let mut redacting = RedactingVisitor::new(&mut visitor);
        event.record(&mut redacting);
        let mut fields = visitor.into_map();

        // Event field names already spoken for, and so skipped when the rest of
        // the map is flattened to the root below. Every one of them is emitted
        // under a *different* key — `error` as `error_message`, `name` as the
        // `message` fallback — and a field that appears both under its own name
        // and under its promoted one is the same value billed twice: once to a
        // column, once to the attributes blob, with nothing to say they are one
        // fact.
        let mut consumed: Vec<&'static str> = Vec::new();

        // An event that names one of the promoted fields itself outranks the
        // span stack: it is the more specific statement, and the only reason to
        // write it on an event is to correct what the span says.
        let mut event_promoted = serde_json::Map::new();
        for (field, column) in PROMOTED_SPAN_FIELDS {
            if let Some(value) = fields.get(*field) {
                event_promoted.insert((*column).to_string(), value.clone());
                consumed.push(field);
            }
        }
        let mut promoted = serde_json::Map::new();

        // The span stack flattened root→innermost, and the name of the
        // innermost span.
        //
        // `spans` below keeps the nesting for a human reading raw lines; this
        // map exists because the collector cannot see into an array. Merging
        // root→innermost gives the innermost span the last word on a repeated
        // key while a field only an *outer* span carries still survives:
        // `hydradb.scope` is recorded on `index.scope`, and an event that
        // fires three frames down inside `artifact.publish` would otherwise
        // carry no scope at all. The duplication against `spans` is deliberate
        // and paid for knowingly.
        let mut span_merged = serde_json::Map::new();
        let mut operation: Option<String> = None;

        // The enclosing span stack, innermost last. Span fields were already
        // redacted when `RedactingFields` formatted them.
        if let Some(scope) = ctx.event_scope() {
            let mut spans: Vec<serde_json::Value> = Vec::new();
            for span in scope.from_root() {
                // Root-first, so the last writer is the innermost span — which
                // is the one `operation` means. Span names are already
                // `subsystem.verb` (`index.cycle`, `artifact.publish`), the
                // same shape the other services write to this column, so no
                // massaging is needed to make them comparable.
                operation = Some(span.name().to_string());
                let mut entry = serde_json::Map::new();
                entry.insert("name".to_string(), serde_json::Value::String(span.name().to_string()));
                let extensions = span.extensions();
                if let Some(formatted) = extensions.get::<FormattedFields<N>>() {
                    if let Ok(serde_json::Value::Object(parsed)) = serde_json::from_str::<serde_json::Value>(formatted.fields.as_str()) {
                        for (key, value) in parsed {
                            // Walking root-first means the innermost span
                            // writes last and wins, which is what a nested
                            // scope should do.
                            if let Some((_, column)) = PROMOTED_SPAN_FIELDS.iter().find(|(field, _)| *field == key.as_str()) {
                                promoted.insert((*column).to_string(), value.clone());
                            }
                            // The synthetic `name` above is never merged in: it
                            // is `operation`, and copying it here would make
                            // `span.name` mean "the innermost span" while an
                            // actual `name` *field* on some span means
                            // something else entirely.
                            span_merged.insert(key.clone(), value.clone());
                            entry.insert(key, value);
                        }
                    }
                }
                spans.push(serde_json::Value::Object(entry));
            }
            if !spans.is_empty() {
                line.insert("spans".to_string(), serde_json::Value::Array(spans));
            }
        }
        promoted.extend(event_promoted);

        // `message` is just another field to `tracing`; promoting it to the top
        // level is what makes the line readable in a log viewer.
        //
        // It is also emitted *unconditionally*, and that is the fix for the
        // worst thing these logs did: nearly every ERROR row carried the
        // entire raw JSON log line in `message`. The collector merges the
        // parsed keys over a record whose `message` starts out as the raw line,
        // so a line with no `message` key of its own never overwrites it and
        // the escape hatch becomes the value. `tracing::error!(error = …, name
        // = …)` — no format string — produces exactly that, and the offenders
        // are third-party (`opentelemetry_sdk`, and slatedb through the `log`
        // bridge), so no amount of fixing our own call sites reaches them.
        //
        // The fallbacks descend from most to least specific and end at
        // `meta.target()`, which always exists, so the key cannot be missing.
        let message = match fields.remove("message") {
            Some(value) => text_of(&value),
            None => {
                // Event before span at each rung: the event is closer to the
                // call site, so its `error` is the one being reported.
                let fallback = ["error", "name"]
                    .into_iter()
                    .find_map(|key| fields.get(key).or_else(|| span_merged.get(key)).map(|value| (key, text_of(value))));
                match fallback {
                    Some((key, text)) => {
                        consumed.push(key);
                        text
                    }
                    None => meta.target().to_string(),
                }
            }
        };
        line.insert("message".to_string(), serde_json::Value::String(message));

        // `error_type` when nothing classified itself, so that an error is at
        // least *groupable*.
        //
        // Every branch here is a bounded vocabulary, and that is a hard
        // requirement rather than tidiness: the column is
        // `LowCardinality(String)`, which ClickHouse encodes as a per-part
        // dictionary and which degrades — badly, and permanently for those
        // parts — once the distinct count runs away. `error.class` is an
        // 11-variant enum, `name` is the OTel SDK's own internal error name,
        // `target` is a Rust module path: all three are enumerable from the
        // source tree. The error *text* is never a candidate, however tempting,
        // because it carries ids, paths and byte counts; unbounded text is what
        // `error_message` is for.
        //
        // Gated on ERROR and WARN because an `error_type` on an INFO line would
        // make every healthy line look classified, and the column's whole use
        // is `WHERE error_type = …` over the ones that failed.
        if !promoted.contains_key("error_type") && matches!(*meta.level(), tracing::Level::ERROR | tracing::Level::WARN) {
            let class = match fields.get("name").or_else(|| span_merged.get("name")) {
                Some(value) => {
                    consumed.push("name");
                    text_of(value)
                }
                None => meta.target().to_string(),
            };
            promoted.insert("error_type".to_string(), serde_json::Value::String(class));
        }

        line.extend(promoted);
        if let Some(operation) = operation {
            line.insert("operation".to_string(), serde_json::Value::String(operation));
        }
        if !span_merged.is_empty() {
            line.insert("span".to_string(), serde_json::Value::Object(span_merged));
        }

        // Event fields land at the *root*, not under a `fields` object, for the
        // same reason the promoted table exists: the collector maps root keys
        // to columns and buckets everything nested into an opaque blob. Nesting
        // them was making every field of every event unqueryable by name.
        //
        // A field whose name the line already owns keeps the old nesting rather
        // than overwriting it — see [`RESERVED_ROOT_KEYS`] — so the object is
        // absent entirely in the common case and present, small, and lossless
        // in the case that used to be a silent clobber.
        let mut nested = serde_json::Map::new();
        for (key, value) in fields {
            if consumed.iter().any(|taken| *taken == key) {
                continue;
            }
            if is_reserved_root_key(&key) {
                nested.insert(key, value);
            } else {
                line.insert(key, value);
            }
        }
        if !nested.is_empty() {
            line.insert("fields".to_string(), serde_json::Value::Object(nested));
        }

        let rendered = serde_json::to_string(&serde_json::Value::Object(line)).map_err(|_| fmt::Error)?;
        writeln!(writer, "{rendered}")
    }
}
