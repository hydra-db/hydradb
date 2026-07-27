//! The head sampler.
//!
//! # What a head sampler can and cannot do
//!
//! The plan (§6) asks for "always keep a trace that contains an error". A head
//! sampler cannot honour that, and it is worth being exact about why rather
//! than shipping something that looks like it does: [`ShouldSample`] runs when
//! a span *starts*, and whether the operation fails is not known for another
//! few milliseconds. By the time the error status is set, the sampling decision
//! has already been made and the child spans have already been dropped.
//!
//! Keeping error traces is genuinely a *tail* decision, and it belongs in the
//! collector's tail-sampling processor, which buffers a whole trace before
//! deciding. That is a deployment change, not a code change, and it is the
//! right place for it. The deployment requirement is spelled out under
//! [*What the collector must do*](#what-the-collector-must-do) below.
//!
//! What this sampler does instead is honour the decisions that *are* knowable
//! at span start:
//!
//! 1. **Respect the parent.** A sampled parent means a sampled child, so a
//!    distributed query is never half-recorded.
//! 2. **Always keep the rare-and-interesting.** Writer promotions and fence
//!    refreshes are low-volume and are the entire reason the write path is
//!    being instrumented; sampling them at 5% would mean waiting twenty
//!    incidents to see one.
//! 3. **Honour an explicit request.** A caller that knows *before it starts the
//!    work* that a span is worth keeping sets [`semconv::SAMPLING_FORCE`] as a
//!    creation-time attribute and the trace is kept.
//! 4. **Ratio-sample everything else**, deterministically on the trace id, so
//!    every service in a trace independently reaches the same answer.
//!
//! # The two limits on rule 3, precisely
//!
//! Rule 3 has two structural limits. Neither is a bug to be fixed here; both
//! are consequences of head sampling, and both are easy to write code against
//! by accident, so they are stated rather than left to be rediscovered.
//!
//! **The attribute must exist when the span starts.** `tracing` spans routinely
//! declare a field as `tracing::field::Empty` and fill it in later with
//! `Span::record`. That value never reaches [`ShouldSample`] — by the time it is
//! recorded the span has been entered and the decision has been taken — so
//! `span.record(SAMPLING_FORCE, true)` is a silent no-op. Worse, it is a
//! *conditionally* silent one: recording before the span is first entered
//! happens to land in the builder, so the same line can appear to work in one
//! function and do nothing in the next. Do not rely on the ordering; set the
//! attribute at creation or do not set it at all.
//!
//! **A child cannot overrule its parent.** Rule 1 wins over rule 3, so an
//! explicit force on a non-root span does nothing when the root was dropped.
//! That is correct: the surviving child would be a trace with a hole where its
//! parent should be, missing the scope, fingerprint and correlation id that make
//! it interpretable. A force is a statement about a *trace*, and only the span
//! that starts one can make it.
//!
//! Together these mean the force attribute serves exactly one shape of call
//! site: a root span whose keep-worthiness is known before the work begins.
//! Every site that discovers keep-worthiness while the work runs — a failure, a
//! full scan the planner only just found — is out of reach, permanently, and
//! records [`semconv::SAMPLING_TAIL_KEEP`] instead.
//!
//! # What the collector must do
//!
//! [`semconv::SAMPLING_TAIL_KEEP`] is inert without a collector-side policy;
//! nothing in this process acts on it. The deployment must run a
//! `tail_sampling` processor with a policy that keeps any trace containing a
//! span carrying it, for example:
//!
//! ```yaml
//! processors:
//!   tail_sampling:
//!     decision_wait: 10s
//!     policies:
//!       - name: keep-flagged
//!         type: string_attribute
//!         string_attribute:
//!           key: turbolay.sampling.tail_keep
//!           values: [error, full_scan]
//!       - name: baseline
//!         type: probabilistic
//!         probabilistic: { sampling_percentage: 5 }
//! ```
//!
//! One consequence has to be accepted rather than worked around: a trace the
//! *head* sampler dropped never reaches the collector at all, so the tail policy
//! can only rescue traces this sampler kept. Running the head sampler at a ratio
//! of 1.0 and doing all the thinning in the collector is the configuration that
//! makes the tail policy fully effective, and it is the one to choose when
//! error-trace coverage matters more than export bandwidth.

use opentelemetry::trace::{Link, SpanKind, TraceContextExt, TraceId};
use opentelemetry::{Context, KeyValue};
use opentelemetry_sdk::trace::{SamplingDecision, SamplingResult, ShouldSample};

use crate::semconv;

/// Span names that are always sampled regardless of ratio.
///
/// All three are low-volume and high-value: a promotion or a fence refresh is
/// the writer ping-pong actually happening, and dropping 95% of those would
/// defeat the reason for tracing the write path at all.
const ALWAYS_SAMPLE_SPANS: &[&str] = &["writer.promote", "writer.fence_refresh", "writer.acquire"];

/// Turbolay's head sampling policy. See the module docs.
#[derive(Clone, Debug)]
pub struct TurbolaySampler {
    ratio: f64,
}

impl TurbolaySampler {
    /// Build with a ratio in `0.0..=1.0`. Values outside the range are clamped
    /// rather than rejected — a sampler is not worth failing a boot over.
    pub fn new(ratio: f64) -> Self {
        Self {
            ratio: ratio.clamp(0.0, 1.0),
        }
    }

    /// The configured ratio, after clamping.
    pub fn ratio(&self) -> f64 {
        self.ratio
    }

    /// Whether a trace-starting span is kept irrespective of ratio.
    ///
    /// `attributes` is what the span was *created* with. Only
    /// [`semconv::SAMPLING_FORCE`] is consulted, and only because it exists for
    /// no other purpose: a sampler that keys off a *data* attribute silently
    /// couples retention volume to a workload property, so the day somebody
    /// hoists that field to creation time the sampling ratio quietly becomes
    /// 100% for a whole class of query. The force flag says "keep this" and
    /// nothing else, which is why it is the only thing read here.
    fn is_always_sampled(name: &str, attributes: &[KeyValue]) -> bool {
        if ALWAYS_SAMPLE_SPANS.contains(&name) {
            return true;
        }
        attributes.iter().any(|attribute| {
            attribute.key.as_str() == semconv::SAMPLING_FORCE
                // Both spellings, because `info_span!(force = true)` arrives as
                // a bool and a stringly-typed caller arrives as "true".
                && attribute.value.as_str() == "true"
        })
    }

    /// Deterministic ratio test on the trace id.
    ///
    /// Uses the low 8 bytes, per the W3C/OTel convention, so two services that
    /// see the same trace id independently reach the same decision — which is
    /// what stops a trace from being sampled on one node and dropped on the
    /// next.
    fn ratio_allows(&self, trace_id: TraceId) -> bool {
        if self.ratio >= 1.0 {
            return true;
        }
        if self.ratio <= 0.0 {
            return false;
        }
        let bytes = trace_id.to_bytes();
        let low = u64::from_be_bytes([
            bytes[8], bytes[9], bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15],
        ]);
        // Shift away the sign bit so the comparison stays in the positive half
        // of the space, matching the upstream `TraceIdRatioBased` sampler.
        (low >> 1) < (self.ratio * (1u64 << 63) as f64) as u64
    }
}

impl ShouldSample for TurbolaySampler {
    fn should_sample(
        &self,
        parent_context: Option<&Context>,
        trace_id: TraceId,
        name: &str,
        _span_kind: &SpanKind,
        attributes: &[KeyValue],
        _links: &[Link],
    ) -> SamplingResult {
        let parent_span_context =
            parent_context.map(|context| context.span().span_context().clone());

        // A remote or local parent decides for the whole trace. Re-deciding per
        // span is what produces traces with holes in the middle.
        let decision = match &parent_span_context {
            Some(parent) if parent.is_valid() => {
                if parent.is_sampled() {
                    SamplingDecision::RecordAndSample
                } else {
                    SamplingDecision::Drop
                }
            }
            _ => {
                if Self::is_always_sampled(name, attributes) || self.ratio_allows(trace_id) {
                    SamplingDecision::RecordAndSample
                } else {
                    SamplingDecision::Drop
                }
            }
        };

        SamplingResult {
            decision,
            attributes: Vec::new(),
            trace_state: parent_span_context
                .map(|parent| parent.trace_state().clone())
                .unwrap_or_default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn trace_id_from(low: u64) -> TraceId {
        let mut bytes = [0u8; 16];
        bytes[0..8].copy_from_slice(&1u64.to_be_bytes());
        bytes[8..16].copy_from_slice(&low.to_be_bytes());
        TraceId::from_bytes(bytes)
    }

    fn sample(sampler: &TurbolaySampler, name: &str, attributes: &[KeyValue]) -> SamplingDecision {
        sampler
            .should_sample(
                None,
                trace_id_from(u64::MAX),
                name,
                &SpanKind::Internal,
                attributes,
                &[],
            )
            .decision
    }

    #[test]
    fn ratio_is_clamped_not_rejected() {
        assert_eq!(TurbolaySampler::new(-3.0).ratio(), 0.0);
        assert_eq!(TurbolaySampler::new(7.0).ratio(), 1.0);
        assert_eq!(TurbolaySampler::new(0.25).ratio(), 0.25);
    }

    #[test]
    fn ratio_one_keeps_everything() {
        let sampler = TurbolaySampler::new(1.0);
        for low in [0, 1, u64::MAX / 2, u64::MAX] {
            assert_eq!(
                sampler
                    .should_sample(None, trace_id_from(low), "q", &SpanKind::Internal, &[], &[])
                    .decision,
                SamplingDecision::RecordAndSample
            );
        }
    }

    /// Ratio zero must still keep the always-sampled spans, or the write path
    /// goes dark exactly when somebody turns sampling off to cut cost.
    #[test]
    fn ratio_zero_still_keeps_writer_spans() {
        let sampler = TurbolaySampler::new(0.0);
        assert_eq!(
            sample(&sampler, "client.query", &[]),
            SamplingDecision::Drop
        );
        assert_eq!(
            sample(&sampler, "writer.promote", &[]),
            SamplingDecision::RecordAndSample
        );
        assert_eq!(
            sample(&sampler, "writer.fence_refresh", &[]),
            SamplingDecision::RecordAndSample
        );
    }

    #[test]
    fn explicit_force_attribute_is_honoured() {
        let sampler = TurbolaySampler::new(0.0);
        for value in [
            KeyValue::new(semconv::SAMPLING_FORCE, "true"),
            KeyValue::new(semconv::SAMPLING_FORCE, true),
        ] {
            assert_eq!(
                sample(&sampler, "anything", &[value]),
                SamplingDecision::RecordAndSample
            );
        }
    }

    /// `force = false` is the common case and must not force a keep.
    #[test]
    fn a_false_flag_does_not_force_a_keep() {
        let sampler = TurbolaySampler::new(0.0);
        let attributes = [KeyValue::new(semconv::SAMPLING_FORCE, "false")];
        assert_eq!(
            sample(&sampler, "query.plan", &attributes),
            SamplingDecision::Drop
        );
    }

    /// `turbolay.query.full_scan` is a *data* attribute, not a sampling one.
    ///
    /// It used to force a keep here, which was dead code — it is only ever
    /// recorded after the planner has run, on a child span, so the sampler never
    /// saw it. Reviving it by hoisting the field to creation time would be worse
    /// than the dead code: full scans are not rare in an analytics workload, and
    /// the sampling ratio would silently become 100% for all of them. Keep
    /// worthiness is stated with [`semconv::SAMPLING_FORCE`], which means that
    /// and nothing else.
    #[test]
    fn a_data_attribute_never_forces_a_keep() {
        let sampler = TurbolaySampler::new(0.0);
        let attributes = [KeyValue::new(semconv::QUERY_FULL_SCAN, "true")];
        assert_eq!(
            sample(&sampler, "query.plan", &attributes),
            SamplingDecision::Drop
        );
    }

    /// The tail marker is the collector's input and must be inert here. If this
    /// ever starts passing as `RecordAndSample`, the two mechanisms have been
    /// conflated again and post-hoc call sites will once more look like they
    /// force a keep.
    #[test]
    fn the_tail_keep_marker_is_inert_in_the_head_sampler() {
        let sampler = TurbolaySampler::new(0.0);
        let attributes = [KeyValue::new(
            semconv::SAMPLING_TAIL_KEEP,
            semconv::SAMPLING_TAIL_KEEP_ERROR,
        )];
        assert_eq!(
            sample(&sampler, "client.query", &attributes),
            SamplingDecision::Drop
        );
    }

    /// The decision must depend only on the trace id, so every node in a
    /// distributed query independently agrees.
    #[test]
    fn ratio_decision_is_deterministic_in_the_trace_id() {
        let sampler = TurbolaySampler::new(0.5);
        let id = trace_id_from(12345);
        let first = sampler.should_sample(None, id, "q", &SpanKind::Internal, &[], &[]);
        let second = sampler.should_sample(None, id, "q", &SpanKind::Internal, &[], &[]);
        assert_eq!(first.decision, second.decision);
    }

    #[test]
    fn ratio_selects_roughly_the_requested_share() {
        let sampler = TurbolaySampler::new(0.25);
        let kept = (0..10_000u64)
            .filter(|low| {
                sampler
                    .should_sample(
                        None,
                        trace_id_from(low.wrapping_mul(0x9E37_79B9_7F4A_7C15)),
                        "q",
                        &SpanKind::Internal,
                        &[],
                        &[],
                    )
                    .decision
                    == SamplingDecision::RecordAndSample
            })
            .count();
        // Wide bounds: this asserts the ratio is applied at all, not that the
        // hash is uniform to three decimal places.
        assert!(
            (2_000..3_000).contains(&kept),
            "expected roughly 2500 of 10000, got {kept}"
        );
    }
}
