//! Bridge from slatedb's [`MetricsRecorder`] and foyer's [`RegistryOps`] to the
//! `metrics` crate.
//!
//! Slatedb calls `register_*` during DB construction; foyer calls
//! `register_*_vec` when building a hybrid cache. Both are forwarded to the
//! `metrics` global recorder so that all storage metrics appear alongside
//! application metrics on the Prometheus `/metrics` endpoint.

use std::borrow::Cow;
use std::sync::Arc;

use mixtrics::metrics::{
    BoxedCounter, BoxedCounterVec, BoxedGauge, BoxedGaugeVec, BoxedHistogram, BoxedHistogramVec,
    CounterOps, CounterVecOps, GaugeOps, GaugeVecOps, HistogramOps, HistogramVecOps, RegistryOps,
};
use slatedb_common::metrics::{CounterFn, GaugeFn, HistogramFn, MetricsRecorder, UpDownCounterFn};

/// [`MetricsRecorder`] implementation that forwards to the `metrics` crate.
///
/// Metric names are forwarded as-is (dot-separated, e.g.
/// `slatedb.db.write_ops`). Labels become `metrics::Label`s. Descriptions
/// are registered via `metrics::describe_*`.
pub struct MetricsRsRecorder;

impl MetricsRecorder for MetricsRsRecorder {
    fn register_counter(
        &self,
        name: &str,
        description: &str,
        labels: &[(&str, &str)],
    ) -> Arc<dyn CounterFn> {
        let name = name.to_string();
        let labels = to_labels(labels);
        metrics::describe_counter!(name.clone(), description.to_string());
        Arc::new(CounterHandle(metrics::counter!(name, labels)))
    }

    fn register_gauge(
        &self,
        name: &str,
        description: &str,
        labels: &[(&str, &str)],
    ) -> Arc<dyn GaugeFn> {
        let name = name.to_string();
        let labels = to_labels(labels);
        metrics::describe_gauge!(name.clone(), description.to_string());
        Arc::new(GaugeHandle(metrics::gauge!(name, labels)))
    }

    fn register_up_down_counter(
        &self,
        name: &str,
        description: &str,
        labels: &[(&str, &str)],
    ) -> Arc<dyn UpDownCounterFn> {
        // metrics-rs has no up-down counter; map to a gauge with increment.
        let name = name.to_string();
        let labels = to_labels(labels);
        metrics::describe_gauge!(name.clone(), description.to_string());
        Arc::new(UpDownCounterHandle(metrics::gauge!(name, labels)))
    }

    fn register_histogram(
        &self,
        name: &str,
        description: &str,
        labels: &[(&str, &str)],
        _boundaries: &[f64],
    ) -> Arc<dyn HistogramFn> {
        // Bucket boundaries are configured on the PrometheusBuilder, not here.
        let name = name.to_string();
        let labels = to_labels(labels);
        metrics::describe_histogram!(name.clone(), description.to_string());
        Arc::new(HistogramHandle(metrics::histogram!(name, labels)))
    }
}

fn to_labels(labels: &[(&str, &str)]) -> Vec<metrics::Label> {
    labels
        .iter()
        .map(|(k, v)| metrics::Label::new(k.to_string(), v.to_string()))
        .collect()
}

// -- Handle wrappers --

struct CounterHandle(metrics::Counter);

impl CounterFn for CounterHandle {
    fn increment(&self, value: u64) {
        self.0.increment(value);
    }
}

struct GaugeHandle(metrics::Gauge);

impl GaugeFn for GaugeHandle {
    fn set(&self, value: i64) {
        self.0.set(value as f64);
    }
}

struct UpDownCounterHandle(metrics::Gauge);

impl UpDownCounterFn for UpDownCounterHandle {
    fn increment(&self, value: i64) {
        self.0.increment(value as f64);
    }
}

struct HistogramHandle(metrics::Histogram);

impl HistogramFn for HistogramHandle {
    fn record(&self, value: f64) {
        self.0.record(value);
    }
}

// -- Foyer / mixtrics bridge --

/// [`RegistryOps`] implementation that forwards foyer metrics to the `metrics`
/// crate, mirroring what [`MetricsRsRecorder`] does for slatedb.
#[derive(Debug)]
pub struct MixtricsBridge;

impl RegistryOps for MixtricsBridge {
    fn register_counter_vec(
        &self,
        name: Cow<'static, str>,
        desc: Cow<'static, str>,
        label_names: &'static [&'static str],
    ) -> BoxedCounterVec {
        metrics::describe_counter!(name.clone(), desc.into_owned());
        Box::new(MetricsCounterVec { name, label_names })
    }

    fn register_gauge_vec(
        &self,
        name: Cow<'static, str>,
        desc: Cow<'static, str>,
        label_names: &'static [&'static str],
    ) -> BoxedGaugeVec {
        metrics::describe_gauge!(name.clone(), desc.into_owned());
        Box::new(MetricsGaugeVec { name, label_names })
    }

    fn register_histogram_vec(
        &self,
        name: Cow<'static, str>,
        desc: Cow<'static, str>,
        label_names: &'static [&'static str],
    ) -> BoxedHistogramVec {
        metrics::describe_histogram!(name.clone(), desc.into_owned());
        Box::new(MetricsHistogramVec { name, label_names })
    }

    fn register_histogram_vec_with_buckets(
        &self,
        name: Cow<'static, str>,
        desc: Cow<'static, str>,
        label_names: &'static [&'static str],
        _buckets: Vec<f64>,
    ) -> BoxedHistogramVec {
        // Bucket boundaries are configured on the PrometheusBuilder, not here.
        self.register_histogram_vec(name, desc, label_names)
    }
}

fn zip_labels(label_names: &[&str], label_values: &[Cow<'static, str>]) -> Vec<metrics::Label> {
    label_names
        .iter()
        .zip(label_values.iter())
        .map(|(k, v)| metrics::Label::new(k.to_string(), v.to_string()))
        .collect()
}

// -- Vec wrappers --

#[derive(Debug)]
struct MetricsCounterVec {
    name: Cow<'static, str>,
    label_names: &'static [&'static str],
}

impl CounterVecOps for MetricsCounterVec {
    fn counter(&self, labels: &[Cow<'static, str>]) -> BoxedCounter {
        let labels = zip_labels(self.label_names, labels);
        Box::new(MetricsCounter(metrics::counter!(self.name.clone(), labels)))
    }
}

#[derive(Debug)]
struct MetricsGaugeVec {
    name: Cow<'static, str>,
    label_names: &'static [&'static str],
}

impl GaugeVecOps for MetricsGaugeVec {
    fn gauge(&self, labels: &[Cow<'static, str>]) -> BoxedGauge {
        let labels = zip_labels(self.label_names, labels);
        Box::new(MetricsGauge(metrics::gauge!(self.name.clone(), labels)))
    }
}

#[derive(Debug)]
struct MetricsHistogramVec {
    name: Cow<'static, str>,
    label_names: &'static [&'static str],
}

impl HistogramVecOps for MetricsHistogramVec {
    fn histogram(&self, labels: &[Cow<'static, str>]) -> BoxedHistogram {
        let labels = zip_labels(self.label_names, labels);
        Box::new(MetricsHistogram(metrics::histogram!(
            self.name.clone(),
            labels
        )))
    }
}

// -- Individual metric wrappers --

#[derive(Debug)]
struct MetricsCounter(metrics::Counter);

impl CounterOps for MetricsCounter {
    fn increase(&self, val: u64) {
        self.0.increment(val);
    }
}

#[derive(Debug)]
struct MetricsGauge(metrics::Gauge);

impl GaugeOps for MetricsGauge {
    fn increase(&self, val: u64) {
        self.0.increment(val as f64);
    }

    fn decrease(&self, val: u64) {
        self.0.decrement(val as f64);
    }

    fn absolute(&self, val: u64) {
        self.0.set(val as f64);
    }
}

#[derive(Debug)]
struct MetricsHistogram(metrics::Histogram);

impl HistogramOps for MetricsHistogram {
    fn record(&self, val: f64) {
        self.0.record(val);
    }
}
