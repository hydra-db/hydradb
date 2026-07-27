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

/// The JSON event format.
///
/// Every line carries `binary` and `service`, which is what makes a shared log
/// sink separable — see the module docs for why the built-in formatter cannot
/// promise that.
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
            "service".to_string(),
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

        // Event fields, redacted. This is the path the built-in JSON formatter
        // bypasses.
        let mut visitor = JsonFieldVisitor::default();
        let mut redacting = RedactingVisitor::new(&mut visitor);
        event.record(&mut redacting);
        let mut fields = visitor.into_map();

        // `message` is just another field to `tracing`; promoting it to the top
        // level is what makes the line readable in a log viewer.
        if let Some(message) = fields.remove("message") {
            line.insert("message".to_string(), message);
        }
        if !fields.is_empty() {
            line.insert("fields".to_string(), serde_json::Value::Object(fields));
        }

        // The enclosing span stack, innermost last. Span fields were already
        // redacted when `RedactingFields` formatted them.
        if let Some(scope) = ctx.event_scope() {
            let spans: Vec<serde_json::Value> = scope
                .from_root()
                .map(|span| {
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
                                entry.insert(key, value);
                            }
                        }
                    }
                    serde_json::Value::Object(entry)
                })
                .collect();
            if !spans.is_empty() {
                line.insert("spans".to_string(), serde_json::Value::Array(spans));
            }
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
        assert_eq!(lines[0]["service"], "turbolay-graph-indexer");
        assert_eq!(lines[0]["message"], "graph index generation published");
        assert_eq!(lines[0]["fields"]["cell_id"], "cell-7");
    }

    /// The two binaries must be separable by field, not by message text.
    #[test]
    fn the_two_binaries_are_distinguishable() {
        let node = capture_json(ServiceIdentity::GraphNode, || tracing::info!("up"));
        let indexer = capture_json(ServiceIdentity::GraphIndexer, || tracing::info!("up"));
        assert_ne!(node[0]["binary"], indexer[0]["binary"]);
        assert_ne!(node[0]["service"], indexer[0]["service"]);
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
        assert_eq!(lines[0]["fields"]["parameters"], crate::redact::REDACTED);
        assert_eq!(lines[0]["fields"]["cell_id"], "cell-1");
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

    #[test]
    fn typed_fields_keep_their_json_types() {
        let lines = capture_json(ServiceIdentity::GraphNode, || {
            tracing::info!(elapsed_ms = 42u64, full_scan = true, ratio = 0.5f64, "done");
        });
        assert_eq!(lines[0]["fields"]["elapsed_ms"], 42);
        assert_eq!(lines[0]["fields"]["full_scan"], true);
        assert_eq!(lines[0]["fields"]["ratio"], 0.5);
    }

    #[test]
    fn a_message_only_event_has_no_fields_object() {
        let lines = capture_json(ServiceIdentity::GraphNode, || tracing::info!("bare"));
        assert_eq!(lines[0]["message"], "bare");
        assert!(lines[0].get("fields").is_none());
    }
}
