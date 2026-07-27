//! OTLP exporter wiring: the resource, the three providers, and the redaction
//! that sits between them and the network.
//!
//! Three pipelines are installed. Two are fed from the same `tracing` calls:
//!
//! - **Traces**, via `tracing-opentelemetry`, so `#[instrument]` and
//!   `info_span!` become OTel spans.
//! - **Logs**, via `opentelemetry-appender-tracing`, so every existing
//!   `tracing::info!` and `tracing::warn!` in the kernel becomes an OTLP log
//!   record *without touching a single call site*. Because the appender runs
//!   inside the same subscriber, each record is stamped with the enclosing
//!   `trace_id` and `span_id`, so clicking from a log line to its trace works
//!   on day one.
//!
//! The third is not:
//!
//! - **Metrics**, via an [`SdkMeterProvider`] with a `PeriodicReader`. Nothing
//!   `tracing` emits reaches it. The kernel's counters are `AtomicU64`s read by
//!   observable instruments whose callbacks are registered by the binaries
//!   against [`Providers::meter`]; see [`crate::meter`] for the histogram
//!   families and [`crate::semconv::MetricLabel`] for what may be a dimension.
//!   There is no exporter-side layer here because there is no subscriber
//!   involvement at all.
//!
//! The log pipeline is the reason this is a logs-and-traces change rather than
//! a traces change: the 28 existing tracing calls in `src/` start reporting to
//! the collector the moment the binaries call [`crate::init`].

use std::collections::HashMap;
use std::time::Duration;

use opentelemetry::metrics::{Meter, MeterProvider as _};
use opentelemetry::trace::TracerProvider as _;
use opentelemetry::KeyValue;
use opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge;
use opentelemetry_otlp::{WithExportConfig, WithHttpConfig};
use opentelemetry_sdk::logs::SdkLoggerProvider;
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider};
use opentelemetry_sdk::trace::{BatchSpanProcessor, SdkTracerProvider, SpanData, SpanProcessor};
use opentelemetry_sdk::Resource;
use tracing::Subscriber;
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::Layer;

use crate::config::{OtlpProtocol, TelemetryConfig};
use crate::redact;
use crate::sampling::TurbolaySampler;
use crate::TelemetryError;

/// The providers, kept alive by [`crate::TelemetryGuard`].
///
/// All three must outlive the subscriber and all three must be shut down
/// explicitly: batched spans and logs still sitting in a queue at exit are
/// exactly the records that explain why the process is exiting, and the meter's
/// last interval is the one covering the failure.
///
/// The meter provider is additionally the *only* handle to the metrics
/// pipeline. Traces and logs reach it through the installed subscriber, so a
/// caller never needs to touch them; instruments are registered explicitly, so
/// a caller does — hence [`Providers::meter`].
pub struct Providers {
    tracer: SdkTracerProvider,
    logger: SdkLoggerProvider,
    meter: SdkMeterProvider,
}

impl Providers {
    /// A meter to register instruments against.
    ///
    /// `name` is the instrumentation scope, not the metric name: use one per
    /// subsystem registering instruments (`"turbolay.shard"`,
    /// `"turbolay.client"`), so a backend can attribute a series to the code
    /// that produced it.
    pub fn meter(&self, name: &'static str) -> Meter {
        self.meter.meter(name)
    }

    /// Flush and stop all three pipelines.
    pub fn shutdown(self) {
        // Errors here are reported and swallowed. A failed flush during
        // shutdown is worth knowing about but is never worth a non-zero exit
        // code or a panic in a destructor.
        if let Err(error) = self.tracer.shutdown() {
            eprintln!("turbolay-telemetry: tracer shutdown failed: {error}");
        }
        if let Err(error) = self.logger.shutdown() {
            eprintln!("turbolay-telemetry: logger shutdown failed: {error}");
        }
        // The meter goes last for a reason: its shutdown runs one final
        // collection, and an observable callback that reads a snapshot the rest
        // of the process is still updating is better ordered after the pipelines
        // that only drain.
        if let Err(error) = self.meter.shutdown() {
            eprintln!("turbolay-telemetry: meter shutdown failed: {error}");
        }
    }
}

impl std::fmt::Debug for Providers {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Providers").finish_non_exhaustive()
    }
}

/// The layers [`build`] installs: the trace bridge and the log appender.
pub type OtlpLayers<S> = Vec<Box<dyn Layer<S> + Send + Sync>>;

/// Build the OTLP layers.
///
/// Returns an empty layer set and `None` when no endpoint is configured, which
/// is the case in tests and local runs. That path allocates nothing and opens
/// no socket.
pub fn build<S>(
    config: &TelemetryConfig,
) -> Result<(OtlpLayers<S>, Option<Providers>), TelemetryError>
where
    S: Subscriber + for<'a> LookupSpan<'a> + Send + Sync,
{
    let Some(endpoint) = config.otlp_endpoint.clone() else {
        return Ok((Vec::new(), None));
    };

    let resource = build_resource(config);
    let headers: HashMap<String, String> = config.otlp_headers.iter().cloned().collect();

    if config.otlp_protocol == OtlpProtocol::Grpc {
        // The crate is compiled with the HTTP exporter only, so honouring this
        // would need the `grpc-tonic` feature and its tonic/hyper stack. Warn
        // and continue on HTTP rather than refuse to boot: a node that will not
        // start is a worse outcome than one exporting over a different
        // transport than requested, and the warning says exactly what happened.
        eprintln!(
            "turbolay-telemetry: OTEL_EXPORTER_OTLP_PROTOCOL=grpc requires the \
             grpc-tonic feature; falling back to http/protobuf"
        );
    }

    let span_exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_http()
        .with_endpoint(format!("{}/v1/traces", endpoint.trim_end_matches('/')))
        .with_headers(headers.clone())
        .with_timeout(config.export_timeout)
        .build()
        .map_err(|error| TelemetryError::Exporter(error.to_string()))?;

    let log_exporter = opentelemetry_otlp::LogExporter::builder()
        .with_http()
        .with_endpoint(format!("{}/v1/logs", endpoint.trim_end_matches('/')))
        .with_headers(headers.clone())
        .with_timeout(config.export_timeout)
        .build()
        .map_err(|error| TelemetryError::Exporter(error.to_string()))?;

    let metric_exporter = opentelemetry_otlp::MetricExporter::builder()
        .with_http()
        .with_endpoint(format!("{}/v1/metrics", endpoint.trim_end_matches('/')))
        .with_headers(headers)
        .with_timeout(config.export_timeout)
        .build()
        .map_err(|error| TelemetryError::Exporter(error.to_string()))?;

    let tracer_provider = SdkTracerProvider::builder()
        .with_resource(resource.clone())
        .with_sampler(TurbolaySampler::new(config.sample_ratio))
        // Redaction wraps the batch processor rather than replacing it, so the
        // last thing that touches a span before it is queued for the network is
        // the denylist.
        .with_span_processor(RedactingSpanProcessor::new(
            BatchSpanProcessor::builder(span_exporter).build(),
        ))
        .build();

    let logger_provider = SdkLoggerProvider::builder()
        .with_resource(resource.clone())
        .with_batch_exporter(log_exporter)
        .build();

    // `PeriodicReader` — not the `rt-tokio` variant — because it runs the
    // collection on its own OS thread, which is what every observable callback
    // registered against this provider is written against: a plain `Fn` that
    // reads a cached snapshot and never awaits. The blocking HTTP client in the
    // root manifest is load-bearing for the same reason it is for the batch
    // span processor; an async client panics with "no reactor running" on this
    // thread.
    let meter_provider = SdkMeterProvider::builder()
        .with_resource(resource)
        .with_reader(
            PeriodicReader::builder(metric_exporter)
                .with_interval(config.metric_export_interval)
                .build(),
        )
        .build();

    let tracer = tracer_provider.tracer("turbolay");
    let trace_layer = tracing_opentelemetry::layer().with_tracer(tracer);
    let log_layer = OpenTelemetryTracingBridge::new(&logger_provider);

    let layers: OtlpLayers<S> = vec![Box::new(trace_layer), Box::new(log_layer)];

    Ok((
        layers,
        Some(Providers {
            tracer: tracer_provider,
            logger: logger_provider,
            meter: meter_provider,
        }),
    ))
}

/// The OTel resource — the attributes every span and log from this process
/// carries.
fn build_resource(config: &TelemetryConfig) -> Resource {
    let mut attributes = vec![
        KeyValue::new("service.version", config.service_version.clone()),
        KeyValue::new("service.instance.id", config.instance_id.clone()),
        // Redundant with `service.name`, and deliberately so: it keeps the
        // OTLP view and the stdout JSON view queryable the same way.
        KeyValue::new("turbolay.binary", config.identity.binary()),
    ];
    if let Some(environment) = &config.deployment_environment {
        attributes.push(KeyValue::new(
            "deployment.environment.name",
            environment.clone(),
        ));
    }

    Resource::builder()
        .with_service_name(config.identity.service_name())
        .with_attributes(attributes)
        .build()
}

/// A [`SpanProcessor`] that applies the field denylist immediately before spans
/// are handed to the exporter.
///
/// This is the boundary that matters most. Stdout logs stay inside the
/// customer's own cluster; OTLP leaves the process for a third-party backend,
/// so it is the last place a stray tenant value can still be stopped.
#[derive(Debug)]
pub struct RedactingSpanProcessor<P> {
    inner: P,
}

impl<P: SpanProcessor> RedactingSpanProcessor<P> {
    /// Wrap a processor.
    pub fn new(inner: P) -> Self {
        Self { inner }
    }
}

impl<P: SpanProcessor> SpanProcessor for RedactingSpanProcessor<P> {
    fn on_start(&self, span: &mut opentelemetry_sdk::trace::Span, cx: &opentelemetry::Context) {
        self.inner.on_start(span, cx);
    }

    fn on_end(&self, mut span: SpanData) {
        for attribute in span.attributes.iter_mut() {
            if redact::is_redacted(attribute.key.as_str()) {
                attribute.value = redact::REDACTED.into();
            }
        }
        // Span events carry their own attributes and are just as capable of
        // holding a parameter map.
        for event in span.events.events.iter_mut() {
            for attribute in event.attributes.iter_mut() {
                if redact::is_redacted(attribute.key.as_str()) {
                    attribute.value = redact::REDACTED.into();
                }
            }
        }
        self.inner.on_end(span);
    }

    fn force_flush(&self) -> opentelemetry_sdk::error::OTelSdkResult {
        self.inner.force_flush()
    }

    fn shutdown_with_timeout(&self, timeout: Duration) -> opentelemetry_sdk::error::OTelSdkResult {
        self.inner.shutdown_with_timeout(timeout)
    }

    fn set_resource(&mut self, resource: &Resource) {
        self.inner.set_resource(resource);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ServiceIdentity;
    use opentelemetry::trace::{SpanContext, SpanKind, Status};
    use std::sync::{Arc, Mutex};

    /// Captures whatever the redacting processor forwards.
    #[derive(Debug, Default, Clone)]
    struct Capture(Arc<Mutex<Vec<SpanData>>>);

    impl SpanProcessor for Capture {
        fn on_start(
            &self,
            _span: &mut opentelemetry_sdk::trace::Span,
            _cx: &opentelemetry::Context,
        ) {
        }

        fn on_end(&self, span: SpanData) {
            self.0.lock().unwrap().push(span);
        }

        fn force_flush(&self) -> opentelemetry_sdk::error::OTelSdkResult {
            Ok(())
        }

        fn shutdown_with_timeout(
            &self,
            _timeout: Duration,
        ) -> opentelemetry_sdk::error::OTelSdkResult {
            Ok(())
        }
    }

    fn span_with(attributes: Vec<KeyValue>) -> SpanData {
        SpanData {
            span_context: SpanContext::empty_context(),
            parent_span_id: opentelemetry::trace::SpanId::INVALID,
            parent_span_is_remote: false,
            span_kind: SpanKind::Internal,
            name: "client.query".into(),
            start_time: std::time::SystemTime::UNIX_EPOCH,
            end_time: std::time::SystemTime::UNIX_EPOCH,
            attributes,
            dropped_attributes_count: 0,
            events: Default::default(),
            links: Default::default(),
            status: Status::Unset,
            instrumentation_scope: Default::default(),
        }
    }

    #[test]
    fn denylisted_span_attributes_never_reach_the_exporter() {
        let capture = Capture::default();
        let processor = RedactingSpanProcessor::new(capture.clone());

        processor.on_end(span_with(vec![
            KeyValue::new(crate::semconv::CELL_ID, "cell-7"),
            KeyValue::new("parameters", "tenant-secret"),
        ]));

        let captured = capture.0.lock().unwrap();
        let rendered = format!("{:?}", captured[0].attributes);
        assert!(
            !rendered.contains("tenant-secret"),
            "value reached the exporter: {rendered}"
        );
        assert!(
            rendered.contains("cell-7"),
            "ordinary attribute was dropped"
        );
        assert!(rendered.contains(redact::REDACTED));
    }

    #[test]
    fn resource_names_the_binary_both_ways() {
        let config = TelemetryConfig::new(ServiceIdentity::GraphIndexer);
        let resource = build_resource(&config);
        assert_eq!(
            resource.get(&opentelemetry::Key::from_static_str("service.name")),
            Some("turbolay-graph-indexer".into())
        );
        assert_eq!(
            resource.get(&opentelemetry::Key::from_static_str("turbolay.binary")),
            Some("graph-indexer".into())
        );
    }

    /// The metrics pipeline is the one whose absence is a *missing type*
    /// rather than a missing series: with `metrics` off in the root manifest's
    /// `opentelemetry-otlp` entry, `MetricExporter` does not exist and the
    /// error reads like a version mismatch. Building against a closed loopback
    /// port exercises the construction of all three exporters without needing a
    /// collector; the interval is set past the test's lifetime so nothing but
    /// the shutdown flush ever tries to connect.
    #[test]
    fn an_endpoint_builds_all_three_pipelines() {
        let mut config = TelemetryConfig::new(ServiceIdentity::GraphNode);
        config.otlp_endpoint = Some("http://127.0.0.1:1".to_string());
        config.export_timeout = Duration::from_millis(50);
        config.metric_export_interval = Duration::from_secs(3_600);

        let (layers, providers) =
            build::<tracing_subscriber::Registry>(&config).expect("exporters must build");
        assert_eq!(layers.len(), 2, "the trace bridge and the log appender");

        let providers = providers.expect("an endpoint means providers");
        // The meter is reachable, which is the whole point of holding it: unlike
        // traces and logs, nothing reaches the metrics pipeline through the
        // subscriber.
        let _meter = providers.meter("turbolay.test");
        providers.shutdown();
    }

    /// No endpoint must mean no exporter, no socket and no error — the path
    /// every test and local run takes.
    #[test]
    fn no_endpoint_builds_nothing() {
        let config = TelemetryConfig::new(ServiceIdentity::GraphNode);
        let (layers, providers) =
            build::<tracing_subscriber::Registry>(&config).expect("must not fail");
        assert!(layers.is_empty());
        assert!(providers.is_none());
    }
}
