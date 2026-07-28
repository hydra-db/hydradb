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
                .event_format(TurbolayJson::new(config)),
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
    fn format_fields<R: RecordFields>(
        &self,
        mut writer: Writer<'writer>,
        fields: R,
    ) -> fmt::Result {
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
    /// [`TurbolayJson::format_event`] fails and drops the span's fields
    /// *entirely*. The span still appears in the `spans` array, with nothing
    /// but its name.
    ///
    /// That is not an edge case here, it is the common path. Attributes only
    /// known once the work is done — `error.class`, `turbolay.writer.epoch`,
    /// the three `last_promoted_*` fields — are declared
    /// `tracing::field::Empty` at span creation and filled with
    /// `Span::record`, which is what makes a healthy request pay nothing for
    /// them. Every one of those spans was losing its creation-time fields in
    /// the JSON logs: `turbolay.cell_id` and `turbolay.node_id` vanished from
    /// precisely the fence spans whose whole purpose is to be grouped by
    /// `cell_id`.
    ///
    /// OTLP was never affected — `tracing-opentelemetry` handles `on_record`
    /// itself — so this silently degraded the stdout logs alone, which is the
    /// half somebody reads at 2am with `grep`.
    fn add_fields(
        &self,
        current: &'writer mut FormattedFields<Self>,
        fields: &tracing::span::Record<'_>,
    ) -> fmt::Result {
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

    fn record_error(
        &mut self,
        field: &tracing::field::Field,
        value: &(dyn std::error::Error + 'static),
    ) {
        self.insert(field, serde_json::Value::String(value.to_string()));
    }
}

/// Span and event fields lifted to the top level of the line, and the key each
/// takes there.
///
/// The log collector maps a line's **root** keys onto warehouse columns by
/// name, and `cortex.cortex_logs_v2` has a column waiting for each entry below.
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
/// the warehouse column has. Two days of staging traffic put `error_type`,
/// `correlation_id` and `operation` at exactly zero populated rows out of
/// 82,807, because every one of them was spelled the `turbolay.*` way and left
/// nested. A rename table is all that gap ever was.
///
/// Values are copied **verbatim**, with no type coercion: `duration_ms` and
/// `result_count` face numeric columns, and the collector coerces them there.
/// Coercing here as well would mean two places to disagree about what
/// `elapsed_ms = "n/a"` means, and the one that runs first would silently win.
///
/// The base64 spellings (`turbolay.tenant.scope_id`) are deliberately not
/// promoted. They have no column, they are what `turbolay.scope` already spells
/// out on the span, and a root key with no column is copied into the attributes
/// map on every single line.
const PROMOTED_SPAN_FIELDS: &[(&str, &str)] = &[
    ("turbolay.tenant_id", "tenant_id"),
    ("turbolay.sub_tenant_id", "sub_tenant_id"),
    ("error.class", "error_type"),
    ("error", "error_message"),
    ("turbolay.correlation_id", "correlation_id"),
    ("turbolay.outcome", "event"),
    ("elapsed_ms", "duration_ms"),
    ("turbolay.query.rows_returned", "result_count"),
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
    RESERVED_ROOT_KEYS.contains(&key)
        || PROMOTED_SPAN_FIELDS
            .iter()
            .any(|(_, column)| *column == key)
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
/// which is the name of the `cortex_logs_v2` column and of the OTel resource
/// attribute every other service already writes. Spelling it `service` on the
/// wire bought a turbolay-only rename rule in the collector config, and a
/// collector rule that exists for exactly one producer is a rule that stops
/// being applied the day somebody rewrites the pipeline.
#[derive(Clone, Debug)]
pub struct TurbolayJson {
    binary: &'static str,
    service: &'static str,
    instance: String,
    version: String,
    environment: Option<String>,
}

impl TurbolayJson {
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

impl<S, N> FormatEvent<S, N> for TurbolayJson
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
{
    fn format_event(
        &self,
        ctx: &FmtContext<'_, S, N>,
        mut writer: Writer<'_>,
        event: &Event<'_>,
    ) -> fmt::Result {
        let meta = event.metadata();
        let mut line = serde_json::Map::new();

        line.insert(
            "timestamp".to_string(),
            serde_json::Value::String(
                chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Micros, true),
            ),
        );
        line.insert(
            "level".to_string(),
            serde_json::Value::String(meta.level().to_string()),
        );
        // Identity first-class on every line, not in a resource block the log
        // sink cannot see.
        line.insert(
            "binary".to_string(),
            serde_json::Value::String(self.binary.to_string()),
        );
        line.insert(
            "service_name".to_string(),
            serde_json::Value::String(self.service.to_string()),
        );
        line.insert(
            "instance".to_string(),
            serde_json::Value::String(self.instance.clone()),
        );
        line.insert(
            "version".to_string(),
            serde_json::Value::String(self.version.clone()),
        );
        if let Some(environment) = &self.environment {
            line.insert(
                "environment".to_string(),
                serde_json::Value::String(environment.clone()),
            );
        }
        line.insert(
            "target".to_string(),
            serde_json::Value::String(meta.target().to_string()),
        );

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
        // `turbolay.scope` is recorded on `index.scope`, and an event that
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
                entry.insert(
                    "name".to_string(),
                    serde_json::Value::String(span.name().to_string()),
                );
                let extensions = span.extensions();
                if let Some(formatted) = extensions.get::<FormattedFields<N>>() {
                    if let Ok(serde_json::Value::Object(parsed)) =
                        serde_json::from_str::<serde_json::Value>(formatted.fields.as_str())
                    {
                        for (key, value) in parsed {
                            // Walking root-first means the innermost span
                            // writes last and wins, which is what a nested
                            // scope should do.
                            if let Some((_, column)) = PROMOTED_SPAN_FIELDS
                                .iter()
                                .find(|(field, _)| *field == key.as_str())
                            {
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
        // worst thing these logs did: 1,559 of 1,570 staging ERROR rows carried
        // the entire raw JSON log line in `message`. The collector merges the
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
                let fallback = ["error", "name"].into_iter().find_map(|key| {
                    fields
                        .get(key)
                        .or_else(|| span_merged.get(key))
                        .map(|value| (key, text_of(value)))
                });
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
        if !promoted.contains_key("error_type")
            && matches!(*meta.level(), tracing::Level::ERROR | tracing::Level::WARN)
        {
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
            line.insert(
                "operation".to_string(),
                serde_json::Value::String(operation),
            );
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

        let rendered =
            serde_json::to_string(&serde_json::Value::Object(line)).map_err(|_| fmt::Error)?;
        writeln!(writer, "{rendered}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ServiceIdentity;
    use std::sync::{Arc, Mutex};

    /// A `MakeWriter` that captures output so a test can assert on the real
    /// formatted line rather than on an intermediate representation.
    #[derive(Clone, Default)]
    struct Capture(Arc<Mutex<Vec<u8>>>);

    impl Capture {
        fn contents(&self) -> String {
            String::from_utf8(self.0.lock().unwrap().clone()).unwrap()
        }
    }

    impl std::io::Write for Capture {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Capture {
        type Writer = Self;

        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    /// Run `body` against a JSON subscriber and return the captured lines.
    fn capture_json(identity: ServiceIdentity, body: impl FnOnce()) -> Vec<serde_json::Value> {
        let capture = Capture::default();
        let config = TelemetryConfig::new(identity);
        let layer = tracing_subscriber::fmt::layer()
            .fmt_fields(RedactingFields::json())
            .event_format(TurbolayJson::new(&config))
            .with_writer(capture.clone());
        let subscriber = tracing_subscriber::registry().with(layer);
        tracing::subscriber::with_default(subscriber, body);
        capture
            .contents()
            .lines()
            .map(|line| serde_json::from_str(line).expect("each line must be valid JSON"))
            .collect()
    }

    #[test]
    fn every_line_is_valid_json_and_names_its_binary() {
        let lines = capture_json(ServiceIdentity::GraphIndexer, || {
            tracing::info!(cell_id = "cell-7", "graph index generation published");
        });
        assert_eq!(lines.len(), 1);
        assert_eq!(lines[0]["binary"], "graph-indexer");
        assert_eq!(lines[0]["service_name"], "turbolay-graph-indexer");
        assert_eq!(lines[0]["message"], "graph index generation published");
        // Root, not under `fields`: the collector maps root keys to columns and
        // buckets anything nested into an opaque blob.
        assert_eq!(lines[0]["cell_id"], "cell-7");
        assert!(lines[0].get("fields").is_none());
    }

    /// With no tracer installed there is no trace to name, and the two keys
    /// must be *absent* rather than present and zero. A zero id looks valid to
    /// the log collector, which would lift it into the OTLP `logRecord` and
    /// produce a deep link to a trace that cannot exist. `tests/log_trace_ids.rs`
    /// covers the other half — that a real tracer does populate them.
    #[test]
    fn no_active_trace_emits_no_trace_ids() {
        let lines = capture_json(ServiceIdentity::GraphNode, || tracing::info!("up"));
        assert!(lines[0].get("trace_id").is_none());
        assert!(lines[0].get("span_id").is_none());
    }

    /// The two binaries must be separable by field, not by message text.
    #[test]
    fn the_two_binaries_are_distinguishable() {
        let node = capture_json(ServiceIdentity::GraphNode, || tracing::info!("up"));
        let indexer = capture_json(ServiceIdentity::GraphIndexer, || tracing::info!("up"));
        assert_ne!(node[0]["binary"], indexer[0]["binary"]);
        assert_ne!(node[0]["service_name"], indexer[0]["service_name"]);
    }

    /// The regression that motivated writing a custom event formatter: the
    /// built-in JSON formatter serialises event fields directly and would let
    /// this through.
    #[test]
    fn event_fields_are_redacted() {
        let lines = capture_json(ServiceIdentity::GraphNode, || {
            tracing::warn!(
                cell_id = "cell-1",
                parameters = "tenant-secret-value",
                "slow query"
            );
        });
        assert_eq!(lines[0]["parameters"], crate::redact::REDACTED);
        assert_eq!(lines[0]["cell_id"], "cell-1");
        assert!(
            !lines[0].to_string().contains("tenant-secret-value"),
            "redacted value leaked into the line"
        );
    }

    #[test]
    fn span_fields_are_redacted_too() {
        let lines = capture_json(ServiceIdentity::GraphNode, || {
            let span = tracing::info_span!("client.query", token = "bearer-abc", cell_id = "c9");
            let _entered = span.enter();
            tracing::info!("executing");
        });
        let line = lines[0].to_string();
        assert!(!line.contains("bearer-abc"), "span field leaked: {line}");
        assert_eq!(lines[0]["spans"][0]["token"], crate::redact::REDACTED);
        assert_eq!(lines[0]["spans"][0]["cell_id"], "c9");
        assert_eq!(lines[0]["spans"][0]["name"], "client.query");
    }

    /// The whole reason the promotion exists: a line logged deep inside a
    /// request, by code that has never heard of a tenant, still names one at
    /// the top level where the warehouse's column mapping can see it.
    #[test]
    fn tenancy_is_promoted_from_the_enclosing_span_to_the_line_root() {
        let lines = capture_json(ServiceIdentity::GraphNode, || {
            let request = tracing::info_span!(
                "client.query",
                turbolay.tenant_id = "l3c4v6lu2w",
                turbolay.sub_tenant_id = "[Gmail]/All Mail",
                turbolay.tenant.scope_id = "bDNjNHY2bHUydw",
            );
            let _request = request.enter();
            let plan = tracing::info_span!("query.plan");
            let _plan = plan.enter();
            tracing::warn!(turbolay.query.full_scan = true, "unindexed access path");
        });
        assert_eq!(lines[0]["tenant_id"], "l3c4v6lu2w");
        assert_eq!(lines[0]["sub_tenant_id"], "[Gmail]/All Mail");
        // The base64 spelling stays on the span. It has no column, and a root
        // key with no column is copied into the attributes map on every line.
        assert!(lines[0].get("tenant.scope_id").is_none());
        assert_eq!(
            lines[0]["spans"][0]["turbolay.tenant.scope_id"],
            "bDNjNHY2bHUydw"
        );
    }

    /// Outside a request there is no tenant, and the keys must be absent rather
    /// than blank: `tenant_id = ""` is a value the warehouse stores and nobody
    /// can then tell from a tenant that genuinely reported nothing.
    #[test]
    fn a_line_outside_any_request_carries_no_tenancy() {
        let lines = capture_json(ServiceIdentity::GraphNode, || {
            tracing::info!("cell writer fence refreshed");
        });
        assert!(lines[0].get("tenant_id").is_none());
        assert!(lines[0].get("sub_tenant_id").is_none());
    }

    /// A sub-tenant is optional — a tenant-level database resolves to a scope
    /// with no third namespace segment — and its absence must not suppress the
    /// tenant that *is* known.
    #[test]
    fn a_tenant_without_a_sub_tenant_still_promotes() {
        let lines = capture_json(ServiceIdentity::GraphNode, || {
            let span = tracing::info_span!("client.query", turbolay.tenant_id = "l3c4v6lu2w");
            span.in_scope(|| tracing::info!("executing"));
        });
        assert_eq!(lines[0]["tenant_id"], "l3c4v6lu2w");
        assert!(lines[0].get("sub_tenant_id").is_none());
    }

    #[test]
    fn span_stack_is_ordered_outermost_first() {
        let lines = capture_json(ServiceIdentity::GraphNode, || {
            let outer = tracing::info_span!("client.query");
            let _outer = outer.enter();
            let inner = tracing::info_span!("query.plan");
            let _inner = inner.enter();
            tracing::info!("planned");
        });
        assert_eq!(lines[0]["spans"][0]["name"], "client.query");
        assert_eq!(lines[0]["spans"][1]["name"], "query.plan");
    }

    /// A field recorded after span creation must not take the rest of the
    /// span's fields with it.
    ///
    /// The whole deferred-attribute pattern rests on this: `error.class`,
    /// `turbolay.writer.epoch` and the `last_promoted_*` fields are declared
    /// `Empty` up front and filled only when the work resolves, so a healthy
    /// request pays nothing. With the default appending `add_fields` the stored
    /// buffer became two concatenated JSON objects, the event formatter's parse
    /// failed, and the span was emitted with its name and *nothing else* —
    /// losing `turbolay.cell_id` from exactly the fence spans that exist to be
    /// grouped by it. Silent, JSON-only, and invisible to OTLP.
    #[test]
    fn recording_a_field_later_keeps_the_fields_set_at_creation() {
        let lines = capture_json(ServiceIdentity::GraphNode, || {
            let span = tracing::info_span!(
                "writer.fence_refresh",
                turbolay.cell_id = "cell-7",
                turbolay.writer.epoch = tracing::field::Empty,
            );
            span.record("turbolay.writer.epoch", 412u64);
            span.in_scope(|| tracing::info!("fenced"));
        });
        assert_eq!(lines[0]["spans"][0]["turbolay.cell_id"], "cell-7");
        assert_eq!(lines[0]["spans"][0]["turbolay.writer.epoch"], 412);
    }

    /// Types survive promotion as well as flattening. `duration_ms` and
    /// `result_count` face numeric warehouse columns, and this formatter does
    /// not coerce for them — it copies the value it was handed, and the
    /// collector coerces once, in one place.
    #[test]
    fn typed_fields_keep_their_json_types() {
        let lines = capture_json(ServiceIdentity::GraphNode, || {
            tracing::info!(elapsed_ms = 42u64, full_scan = true, ratio = 0.5f64, "done");
        });
        assert_eq!(lines[0]["duration_ms"], 42);
        assert_eq!(lines[0]["full_scan"], true);
        assert_eq!(lines[0]["ratio"], 0.5);
    }

    #[test]
    fn a_message_only_event_has_no_fields_object() {
        let lines = capture_json(ServiceIdentity::GraphNode, || tracing::info!("bare"));
        assert_eq!(lines[0]["message"], "bare");
        assert!(lines[0].get("fields").is_none());
    }

    /// The bug this whole change exists for: a line with no `message` key lets
    /// the collector's merge leave the *raw JSON line* sitting in the `message`
    /// column, which is what 1,559 of 1,570 staging ERROR rows contained. Each
    /// rung is exercised because the offending call sites are third-party —
    /// `opentelemetry_sdk` writes `error` and `name`, slatedb comes through the
    /// `log` bridge — so the formatter, not the call site, has to be the thing
    /// that guarantees the key.
    #[test]
    fn message_is_always_present_at_every_fallback_rung() {
        let lines = capture_json(ServiceIdentity::GraphNode, || {
            tracing::error!(error = "boom", name = "ExportError", "explicit message");
            tracing::error!(error = "connection refused", name = "ExportError");
            tracing::error!(name = "BatchLogProcessor.Export.Error");
            tracing::error!(retries = 3);
        });
        assert_eq!(lines[0]["message"], "explicit message");
        // The `error` field's contents, not its quoted JSON rendering.
        assert_eq!(lines[1]["message"], "connection refused");
        assert_eq!(lines[2]["message"], "BatchLogProcessor.Export.Error");
        assert_eq!(lines[3]["message"], "turbolay_telemetry::layers::tests");
        for line in &lines {
            assert!(
                line["message"]
                    .as_str()
                    .is_some_and(|text| !text.is_empty()),
                "message missing or empty: {line}"
            );
        }
    }

    /// A field spent on the `message` fallback must not also appear under its
    /// own name — the same fact billed twice, once to a column and once to the
    /// attributes blob, with nothing to say they are one fact.
    #[test]
    fn a_field_used_as_the_message_fallback_is_not_also_flattened() {
        let lines = capture_json(ServiceIdentity::GraphNode, || {
            tracing::error!(name = "ExportError", retries = 2);
        });
        assert_eq!(lines[0]["message"], "ExportError");
        assert_eq!(lines[0]["retries"], 2);
        assert!(lines[0].get("name").is_none());
        assert!(lines[0].get("fields").is_none());
    }

    /// The renames that turned three warehouse columns from zero populated rows
    /// out of 82,807 into the value the call site already had.
    #[test]
    fn the_rename_table_fills_the_columns_it_was_written_for() {
        let lines = capture_json(ServiceIdentity::GraphNode, || {
            tracing::error!(
                error.class = "storage",
                error = "object store timeout",
                turbolay.correlation_id = "c-42",
                turbolay.outcome = "failure",
                elapsed_ms = 1200u64,
                turbolay.query.rows_returned = 0u64,
                "write failed"
            );
        });
        assert_eq!(lines[0]["error_type"], "storage");
        assert_eq!(lines[0]["error_message"], "object store timeout");
        assert_eq!(lines[0]["correlation_id"], "c-42");
        assert_eq!(lines[0]["event"], "failure");
        assert_eq!(lines[0]["duration_ms"], 1200);
        assert_eq!(lines[0]["result_count"], 0);
        // Promoted, therefore consumed: no key is emitted under both spellings.
        assert!(lines[0].get("error").is_none());
        assert!(lines[0].get("error.class").is_none());
        assert!(lines[0].get("elapsed_ms").is_none());
        assert!(lines[0].get("fields").is_none());
    }

    /// `error_type` is `LowCardinality(String)`, so the fallback rungs are all
    /// bounded vocabularies — here the target, a Rust module path. It fires for
    /// the levels that failed and stays absent on the ones that did not:
    /// classifying a healthy INFO line would make `WHERE error_type = …` match
    /// the whole table.
    #[test]
    fn error_type_falls_back_only_for_failing_levels() {
        let lines = capture_json(ServiceIdentity::GraphNode, || {
            tracing::error!("object store call failed");
            tracing::warn!("retrying");
            tracing::info!("fine");
            tracing::error!(name = "ExportError", "export failed");
            tracing::error!(error.class = "storage", "classified");
        });
        assert_eq!(lines[0]["error_type"], "turbolay_telemetry::layers::tests");
        assert_eq!(lines[1]["error_type"], "turbolay_telemetry::layers::tests");
        assert!(lines[2].get("error_type").is_none());
        // `name` outranks the target: it is the SDK's own error name, which is
        // both bounded and more specific than the module that logged it.
        assert_eq!(lines[3]["error_type"], "ExportError");
        // A real classification is never overwritten by a fallback.
        assert_eq!(lines[4]["error_type"], "storage");
    }

    /// The error *text* is unbounded — ids, paths, byte counts — and must never
    /// reach the low-cardinality column, however convenient it would be.
    #[test]
    fn error_text_never_becomes_the_error_type() {
        let lines = capture_json(ServiceIdentity::GraphNode, || {
            tracing::error!(error = "conditional write failed for key cell-7/00412.sst");
        });
        assert_eq!(
            lines[0]["error_message"],
            "conditional write failed for key cell-7/00412.sst"
        );
        assert_eq!(lines[0]["error_type"], "turbolay_telemetry::layers::tests");
    }

    /// A field named like a root key the line owns must not rewrite it. A
    /// forged `target` or `level` is indistinguishable from the real one once
    /// written, so the collision is kept nested — invisible to the column
    /// mapping, which is the right home for a value that has no column anyway.
    #[test]
    fn a_field_colliding_with_a_reserved_root_key_stays_nested() {
        let lines = capture_json(ServiceIdentity::GraphNode, || {
            tracing::info!(
                target = "not-the-module",
                level = "not-the-level",
                tenant_id = "not-the-tenant",
                cell_id = "cell-3",
                "collision"
            );
        });
        assert_eq!(lines[0]["target"], "turbolay_telemetry::layers::tests");
        assert_eq!(lines[0]["level"], "INFO");
        assert!(lines[0].get("tenant_id").is_none() || lines[0]["tenant_id"] != "not-the-tenant");
        assert_eq!(lines[0]["fields"]["target"], "not-the-module");
        assert_eq!(lines[0]["fields"]["level"], "not-the-level");
        assert_eq!(lines[0]["fields"]["tenant_id"], "not-the-tenant");
        // Non-colliding fields are unaffected and stay at the root.
        assert_eq!(lines[0]["cell_id"], "cell-3");
    }

    /// `operation` is the innermost span's name, which is already the
    /// `subsystem.verb` shape the other services write to this column.
    #[test]
    fn operation_names_the_innermost_span_and_is_absent_without_one() {
        let lines = capture_json(ServiceIdentity::GraphNode, || {
            let outer = tracing::info_span!("index.cycle");
            let _outer = outer.enter();
            let inner = tracing::info_span!("artifact.publish");
            inner.in_scope(|| tracing::info!("published"));
            drop(_outer);
            tracing::info!("idle");
        });
        assert_eq!(lines[0]["operation"], "artifact.publish");
        assert!(lines[1].get("operation").is_none());
        assert!(lines[1].get("span").is_none());
    }

    /// The merged `span` object exists because the collector cannot see into
    /// the `spans` array. Merging root→innermost gives the innermost span the
    /// last word while a field only an outer span carries still survives — the
    /// `turbolay.scope`-set-on-`index.scope` case, where the event fires frames
    /// deeper inside `artifact.publish`.
    #[test]
    fn the_merged_span_object_takes_the_innermost_value_and_keeps_outer_only_fields() {
        let lines = capture_json(ServiceIdentity::GraphNode, || {
            let outer = tracing::info_span!(
                "index.scope",
                turbolay.scope = "acme/mail",
                turbolay.cell_id = "cell-1"
            );
            let _outer = outer.enter();
            let inner = tracing::info_span!("artifact.publish", turbolay.cell_id = "cell-9");
            inner.in_scope(|| tracing::info!("published"));
        });
        assert_eq!(lines[0]["span"]["turbolay.cell_id"], "cell-9");
        assert_eq!(lines[0]["span"]["turbolay.scope"], "acme/mail");
        // The synthetic span name is `operation`, not a merged field.
        assert!(lines[0]["span"].get("name").is_none());
        // `spans` is unchanged: it keeps the nesting a human reads by, and the
        // duplication is an accepted cost.
        assert_eq!(lines[0]["spans"][0]["name"], "index.scope");
        assert_eq!(lines[0]["spans"][0]["turbolay.cell_id"], "cell-1");
        assert_eq!(lines[0]["spans"][1]["turbolay.cell_id"], "cell-9");
    }

    /// An event field outranks the span stack for both the promoted columns and
    /// the merged object's readers: it is closer to the call site, and the only
    /// reason to write it on the event is to correct what the span says.
    #[test]
    fn an_event_field_outranks_the_same_field_on_the_span() {
        let lines = capture_json(ServiceIdentity::GraphNode, || {
            let span = tracing::info_span!("client.query", turbolay.tenant_id = "span-tenant");
            span.in_scope(|| {
                tracing::info!(turbolay.tenant_id = "event-tenant", "executing");
            });
        });
        assert_eq!(lines[0]["tenant_id"], "event-tenant");
        assert_eq!(lines[0]["span"]["turbolay.tenant_id"], "span-tenant");
    }
}
