---
title: OTel metrics, span links and trace-driven alerting
status: draft-for-review
date: 2026-07-26
branch: Turbolay-V3.5
base_commit: 02eba8c
tags:
  - observability
  - opentelemetry
  - metrics
  - cardinality
  - alerting
---

# OTel metrics, span links and trace-driven alerting

Goal: finish the observability story that `docs/plans/2026-07-26-otel-telemetry-crate.md`
started, by taking the four things it deferred and deciding them — metrics
first, because it is the one with a trap in it.

That plan shipped emission for logs and traces (Steps 0–4 and 5a, commits
`0fe1e91`…`02eba8c`). Its §8 and §9 left four things open. This document is
those four, in the order of how much argument they need: **OTel metrics**
(most), **span links from the write path to the indexing path**,
**trace-driven alerting**, and **continuous profiling / span metrics** (least,
and the answer is "not yet").

The one-line summary of the metrics section, since it is long: the periodic
callback shape the prior plan proposed is right, the "leave `/metrics`
untouched" instruction is right, and the premise underneath both — that the
existing `/metrics` endpoint "already works" — is false. It exports 8 of the 65
counters the kernel maintains, and it already uses the unbounded `scope` as a
Prometheus label. The cardinality rule this document is supposed to protect is
already being broken by the code it was told to leave alone.

## Sources

**Prior plans in this repo**

- `docs/plans/2026-07-26-otel-telemetry-crate.md` — the plan this one continues.
  §2 (the attribute registry and its cardinality note), §6 (`error.class`, the
  sampler, redaction-as-a-layer), §8 "Open decisions" and §9 "Explicitly out of
  scope" are the seed of everything below. Its "What implementation falsified"
  section is treated as binding: the mistakes it records are not repeated here,
  and two of them (the exporter's threading model, `span.record` semantics)
  turn out to have direct analogues in the metrics pipeline.
- `docs/plans/2026-07-25-rendezvous-placement.md` — decision 3, the advisory
  cell-writer record, which is where `last_promoted_by` comes from and hence
  what the strongest alert candidate in §3 is built on.
- `docs/plans/2026-07-25-sparse-kernel-backend-consolidation.md` — its "Open
  items" note that no counter exports which sparse-kernel rung served a
  traversal. §1.6 below is where that gets fixed, since it is a metric-shaped
  gap rather than a span-shaped one.

**Code this analysis was derived from, and checked against**

- `src/core/metrics.rs` — `GraphCacheMetricsSnapshot` (:67) and
  `GraphOperationalMetricsSnapshot` (:90), the two kernel counter sets.
- `src/client/service.rs:640` — `ClientQueryMetricsSnapshot`, the third set.
- `src/core/state.rs:903` / `:916` — `GraphCacheEntryCounts` and
  `GraphCacheResidentBytes`, the two gauge sets.
- `src/bin/graph_node/admin.rs` — the whole file; `metrics` (:106) and
  `append_node_metrics` (:145) are what actually reaches Prometheus today.
- `src/bin/graph-indexer.rs` — `IndexerMetrics` (:200) and `indexer_metrics`
  (:935), the second, unrelated metrics vocabulary.
- `src/engine/cluster.rs:1238` and `src/shard/lifecycle.rs:234-320` — the
  collection path, and the reason half of it is lock-free and half is not.
- `src/engine/index_store.rs` — `GraphIndexGeneration` (:11),
  `build_graph_index` (:101), `publish_graph_index` (:236),
  `encode_graph_index_manifest` (:350) and `decode_graph_index_manifest`
  (:364). §2 is entirely about these.
- `src/engine.rs:1213` / `:1227` — `encode_matrix_artifact` /
  `decode_matrix_artifact`, the other persisted manifest.
- `src/shard/query_optimizer.rs:862-989` — `RowQueryPlanSummary`, which is
  where `turbolay.query.access_path` acquires the cardinality the registry
  says it does not have.
- `crates/telemetry/src/semconv.rs`, `redact.rs`, `otlp.rs`, `sampling.rs` —
  the registry, the denylist-as-a-layer precedent, the provider wiring a meter
  provider would join, and the head sampler that constrains exemplars.

**Vendored dependency sources**

- `~/.cargo/registry/.../opentelemetry-0.32.0/src/metrics/instruments/mod.rs:264`
  — `pub type Callback<T> = Box<dyn Fn(&dyn AsyncInstrument<T>) + Send + Sync>`.
  Synchronous. §1.5 turns on this one line.
- `~/.cargo/registry/.../opentelemetry_sdk-0.32.1/src/metrics/internal/aggregate.rs`
  — every data point is constructed with `exemplars: vec![]`. §1.7 turns on
  this.
- `~/.cargo/registry/.../opentelemetry_sdk-0.32.1/src/metrics/periodic_reader.rs:171`
  — `PeriodicReader` spawns its own OS thread, the same hazard that broke the
  span exporter in the prior plan.

**Memory**

- `cell-writer-fencing-pingpong` — three graph-nodes trading one cell's writer.
  The alert in §3 exists to catch this, and §3's honesty about baselines comes
  from the fact that nobody currently knows how often it happens.

## What we found first

Four code reads changed the shape of this document. All four contradict
something the prior plan asserted or implied.

### `/metrics` exports 8 of 65 counters

The prior plan's §8 says metrics are out of scope because
"`GraphOperationalMetricsSnapshot` plus the admin `/metrics` endpoint already
work". The first half is true. The second is not.

The kernel maintains three counter structs, all cumulative `AtomicU64`:

| Struct | Where | Counters |
|---|---|---|
| `GraphOperationalMetricsSnapshot` | `src/core/metrics.rs:90` | 35 |
| `GraphCacheMetricsSnapshot` | `src/core/metrics.rs:67` | 19 |
| `ClientQueryMetricsSnapshot` | `src/client/service.rs:640` | 11 |

(The prior plan says 36 operational counters. It is 35 — `write_attempts`
through `backpressure_waits`, `src/core/metrics.rs:91-125`. A small thing, but
this document is written on the assumption that its own numbers will be
checked.)

`src/bin/graph_node/admin.rs` exports **eight** of those 65. Five come from
`ClientQueryMetricsSnapshot` at :112-130 — `queries_started`,
`queries_completed`, `queries_failed`, `auth_failures`, `scope_denials`. Three
come from `GraphOperationalMetricsSnapshot` at :159-174 —
`query_graphblas_artifact_snapshots`, `query_graphblas_rebuilt_snapshots`,
`query_rust_sparse_fallbacks`. That is the complete list.

Not exported, and therefore invisible in production: every write counter
(`write_attempts`, `write_commits`, `write_retries`); every artifact counter
(`artifact_builds_started`/`_completed`, `artifact_build_duration_us`,
`artifact_publish_batches`, `artifact_records_published`,
`artifact_publish_duration_us`); all four GC counters; all three verifier
counters, including `verifier_failures`; all five `query_rows_*` counters
including `query_rows_failed` and `query_rows_duration_us`; the three
`graph_compute_*` counters; `backpressure_waits`; the five bulk-import timing
counters; and the entire 19-counter `GraphCacheMetricsSnapshot`, whose `cache`
field on `GraphShardRuntimeMetrics` (`src/engine.rs:120`) is read by no binary
at all — `matrix_artifact_hits` appears only in `src/tests.rs:1381` and
`examples/query_bench.rs:981`.

The gauges that *are* exported — `graph_cache_entries` (:175-202) and
`graph_cache_resident_bytes` (:203-229) — come from `GraphCacheEntryCounts` and
`GraphCacheResidentBytes`, which are different structs entirely.

This reframes the whole section. "Mirror the existing snapshot into a meter" is
not a parallel export of something already visible; for 57 of 65 counters it is
the *first* time they leave the process.

### The cardinality trap is already sprung, in `/metrics`

`append_node_metrics` emits every per-shard series with
`{scope="…",cell_id="…"}` — `src/bin/graph_node/admin.rs:161-163` for the three
counters, `:199` for `graph_cache_entries`, `:226` for
`graph_cache_resident_bytes`. `scope` is the tenant root. It is exactly the
attribute `crates/telemetry/src/semconv.rs:22-25` says must never become a
metric dimension, and it has been one for as long as the endpoint has existed.

The rule this document is charged with protecting is therefore not a rule that
is being upheld and might be broken. It is a rule that is being broken, in the
code the prior plan said to leave untouched. §1.4 has to say what to do about
that, not just how to avoid repeating it.

### `turbolay.query.access_path` is not bounded

`crates/telemetry/src/semconv.rs:15-16` lists `QUERY_ACCESS_PATH` among the
attributes that are "bounded by deployment size and are safe anywhere". The
individual labels are bounded: `access_path_label`
(`src/shard/query_optimizer.rs:991`) returns one of about ten variants,
qualified by an edge type, label or property *name* — all schema, all bounded.

But the attribute is not one label. `RowQueryPlanSummary::access_paths`
(`:932`) is `self.access_paths.join(",")` over a `Vec<String>` that accumulates
one entry per planned pattern (`absorb_pattern`, `:875`). The recorded value
for a five-pattern query is a five-element comma-joined sequence. The
cardinality of the attribute is therefore the number of distinct *ordered
sequences* of access paths, not the number of distinct access paths — a
combinatorial quantity that grows with query complexity, not with schema size.
`turbolay.query.optimizer_passes` (`:936`) has the same shape over a
`BTreeSet`, so its cardinality is the power set of the pass vocabulary.

Both are fine as span attributes. Neither is a metric label. The registry's
doc comment is wrong on this point and §1.4 fixes it.

### `writer.fence_refresh` is on the write path, not off it

`crates/telemetry/src/sampling.rs:38-43` lists `writer.fence_refresh` among the
spans that are "low-volume and high-value" and always sampled. `refresh_writer_fence`
(`src/core/state.rs:395`) is called from exactly two places —
`acquire_local_write_guard` (`src/shard/lifecycle.rs:262`) and
`validate_write_fence` (`:523`) — and `acquire_local_write_guard` has 29 call
sites. It runs once per write, not once per incident.

The always-sample rule only fires for spans with no valid parent
(`sampling.rs:115-130`), so under a `client.mutate` root the parent decides and
nothing is over-sampled. But the *span* is high-volume, and it matters for §3:
the ping-pong alert cannot be "fence refreshes are happening". The
distinguishing fields — `turbolay.writer.last_promoted_by` and its two
companions — are recorded only in the fence arm (`src/core/state.rs:426-442`,
via `log_fence_attribution` at `:455`), so they are present on a small subset
of a large population. Any alert must key on those, and any dashboard must
carry both numbers or the rate will look catastrophic.

## 1. OTel metrics

### 1.1 Recommendation

Add an OTel meter provider to `crates/telemetry`, fed by **observable
(asynchronous) instruments** whose callbacks read a snapshot cached by a
separate tokio task. Export **all 65 counters and both gauge sets**, from both
binaries, under one naming scheme. **Duplicate `/metrics` rather than replace
it**, and treat that duplication as permanent, not transitional. Enforce the
label allowlist with a type in `semconv.rs`, not a convention.

The rest of this section argues each of those.

### 1.2 The two binaries have unrelated metrics, and that is the smaller half

`graph-indexer` has its own `IndexerMetrics` (`src/bin/graph-indexer.rs:200`):
`ready`, `cycles`, `successful_cycles`, `failed_cycles`, `open_failures`,
`generations_published`, `generation_failures`, `generations_deleted`,
`last_success_ms` — nine values, served from `indexer_metrics` (`:935`) on its
own admin server (`:903`).

Two differences from `graph-node`, and they are not equally important.

**The naming prefixes differ** (`graph_indexer_*` versus `graph_*`). Cosmetic;
a backend can cope.

**The dimensionality differs completely.** Every indexer series is
process-global. Not one carries `scope`, `cell_id` or `edge_type`, even though
the cycle now knows all three — `IndexFailure` (`src/bin/graph-indexer.rs:36`)
carries `scope`, `cell_id` and `edge_type` as structured fields specifically so
they survive to the top, and the span tree records them. So the indexer's
*traces* can say "cell-7/RELATES failed" and its *metrics* can only say "a
generation failed". `graph_indexer_generation_failures` going up tells you
nothing you can act on without opening the traces.

This is the more interesting gap, and it is the one the meter should close:
`generations_published`, `generation_failures` and `generations_deleted` should
be dimensioned by `cell_id` and `edge_type`; `cycles`, `successful_cycles`,
`failed_cycles` and `ready` should stay process-global because they describe
the process. Note that `scope` is deliberately *not* on that list — see §1.4.

Do not unify the two binaries' vocabularies beyond that. They measure different
things, and the OTel `service.name` resource attribute
(`crates/telemetry/src/otlp.rs:166-169`) already separates them at the backend
without either side renaming anything.

### 1.3 Which attributes may be labels

The registry (`crates/telemetry/src/semconv.rs`) currently classifies its 26
keys informally, in a doc comment, into "bounded and safe anywhere",
"bounded in practice", and "spans only". That is not enough resolution to build
a meter on. The classification below is the one to encode.

**Safe as metric labels.**

| Attribute | Bound | Evidence |
|---|---|---|
| `turbolay.placement.ownership` | 4 | closed vocabulary `local`/`remote`/`unowned`/`unknown`, `semconv.rs:105-119`, recorded at `src/engine/cluster.rs:528-551` |
| `turbolay.kernel` | 3 | the sparse-kernel ladder: `Adjacency`, `CompactCsc`, `SuiteSparse` |
| `turbolay.outcome` | 3 | `Outcome` — success / skipped / failed |
| `error.class` | 11 | `ErrorClass`, `crates/telemetry/src/error_class.rs:29-58` |
| `turbolay.cell_id` | cells per node | configured, `GRAPH_CELLS`, `src/bin/graph-indexer.rs:231` |
| `turbolay.edge_type` | schema | relationship types the tenant defines |

The first four are closed enums and need no further argument. The last two are
the ones worth being precise about: they are bounded per *node*, not globally,
and the product `cell_id × edge_type` is what a per-shard series costs. A node
serving 8 cells with 12 edge types produces 96 series per instrument. With 57
newly-exported counters that is 5,472 series per node before any other
dimension, which is affordable but is not free, and it is the number that
should be checked against the backend's quota before Step 1 ships rather than
after.

**Span-only, and why.**

| Attribute | Why not a label |
|---|---|
| `turbolay.scope` | one value per tenant, unbounded by product decision |
| `turbolay.correlation_id` | one value per request, unbounded by construction; `semconv.rs:127-141` already says this in the strongest terms |
| `turbolay.caller.step` | caller-supplied and untrusted. The prior plan's own worked example is `delete_source.delete_relates_batch_3_0` — a *batch index* inside the label. Unbounded by construction, and supplied by a process Turbolay does not control |
| `turbolay.query.access_path`, `turbolay.query.optimizer_passes` | comma-joined sequences, combinatorial — see "What we found first" |
| `turbolay.query.fingerprint` | bounded by distinct query shapes, which for a fixed application is small and for a multi-tenant engine accepting arbitrary Cypher is not. Not safe *by construction*, only by hoping about the workload |
| `turbolay.read_epoch`, `turbolay.commit_epoch`, `turbolay.base_sequence`, `turbolay.writer.epoch`, `turbolay.writer.last_promoted_epoch`, `turbolay.writer.last_promoted_at` | monotonic. A new value on every write or every tick |
| `turbolay.generation` | a SHA-256 hex digest, `src/engine/index_store.rs:129`. A new value on every rebuild |
| `turbolay.query.rows_estimated`, `turbolay.query.rows_returned` | measurements. They are what a histogram *records*, not what it is keyed by |
| `turbolay.sampling.force` | a sampler control, meaningless to a meter |

**Two that need a decision rather than a rule.**

`turbolay.node_id` and `turbolay.writer.last_promoted_by` are both node
identities, bounded by fleet size (`GRAPH_NODE_ID`, default `graph-node-0`,
`src/bin/graph_node/config.rs:131`). Bounded at any instant; unbounded over
time if ids churn on rescale, which is what a Deployment does and a StatefulSet
does not.

**Recommendation: neither becomes a metric label.** `node_id` is already
carried on every exported record as the `service.instance.id` resource
attribute (`crates/telemetry/src/otlp.rs:154`), and duplicating a resource
attribute as a metric dimension buys nothing while doubling the churn exposure.
`last_promoted_by` is only interesting as a *distinct count within a window*,
which is a trace-side query (§3) and would be a cardinality bomb as a label.

**`turbolay.query.full_scan` is a bool and is the one genuinely new label
worth adding.** Two values, and a `query_rows_started{full_scan="true"}` rate is
the single most actionable series in this document.

### 1.4 Making the rule structural

The prior plan's redaction argument — a rule in one file is auditable, a rule
at 50 call sites is hope (`crates/telemetry/src/redact.rs:1-16`) — applies here
with one difference that changes the mechanism.

Redaction is a *runtime denylist* applied at every sink
(`RedactingVisitor`, `RedactingSpanProcessor` at `otlp.rs:190`) because it
defends against field names invented anywhere in 62k lines of kernel, which
this crate cannot see. Metric labels are attached in one file inside the
telemetry crate, by code this crate owns. Where the compiler can enforce a
rule, a runtime filter is strictly worse: it fails silently on a Tuesday
instead of loudly at `cargo build`.

So: **a newtype, plus a partition test.**

```rust
// semconv.rs
pub struct MetricLabel(&'static str);
impl MetricLabel { pub const fn key(self) -> &'static str { self.0 } }

pub const L_CELL_ID: MetricLabel = MetricLabel(CELL_ID);
pub const L_EDGE_TYPE: MetricLabel = MetricLabel(EDGE_TYPE);
pub const L_KERNEL: MetricLabel = MetricLabel(KERNEL);
pub const L_OUTCOME: MetricLabel = MetricLabel(OUTCOME);
pub const L_ERROR_CLASS: MetricLabel = MetricLabel(ERROR_CLASS);
pub const L_PLACEMENT_OWNERSHIP: MetricLabel = MetricLabel(PLACEMENT_OWNERSHIP);
pub const L_QUERY_FULL_SCAN: MetricLabel = MetricLabel(QUERY_FULL_SCAN);

pub const METRIC_LABELS: &[MetricLabel] = &[/* the seven above */];
pub const SPAN_ONLY_KEYS: &[&str] = &[/* the nineteen others */];
```

The constructor stays private to the module, so `MetricLabel` cannot be built
from an arbitrary string outside `semconv.rs`. Every meter helper takes
`&[(MetricLabel, &str)]` rather than `&[KeyValue]`. Passing `SCOPE` to a metric
is then not a policy violation to be caught in review — it is a type error.

The partition test is what makes it total, and it is the direct descendant of
`no_registry_key_is_redacted` (`semconv.rs:229-237`):

```rust
#[test]
fn every_registry_key_is_classified_exactly_once() {
    for key in ALL_TURBOLAY_KEYS {
        let label = METRIC_LABELS.iter().any(|l| l.key() == *key);
        let span_only = SPAN_ONLY_KEYS.contains(key);
        assert!(label ^ span_only, "{key} is in neither list, or in both");
    }
}
```

A new attribute cannot be added to the registry without deciding, in the same
commit, whether it may be a metric dimension. That is the property worth
having; the newtype is just what makes the decision unforgeable afterwards.

Update the module doc comment at `semconv.rs:13-25` at the same time. It
currently claims `QUERY_ACCESS_PATH` is safe anywhere, which is the one
sentence in the crate that would talk somebody into the wrong thing.

**And the existing violation.** `scope` is a Prometheus label today
(`admin.rs:161`, `:199`, `:226`). Three options: leave it (the endpoint is not
new, and whatever it costs it is already costing), drop it (breaks every
existing dashboard and alert), or leave `/metrics` alone and simply not repeat
it in the OTel export. **Recommend the third.** The prior plan's instruction to
leave `/metrics` untouched is right, and it is right for a reason beyond
caution: `/metrics` is scraped by a Prometheus that is already sized for
whatever tenant count it sees, whereas the OTLP pipeline ships to a vendor
billing per series. The two have different cost functions and should not be
forced to the same dimensionality. Record the divergence in the runbook —
"`scope` is on the Prometheus series and deliberately not on the OTLP series"
is exactly the kind of thing that reads as a bug six months later.

### 1.5 The shape: a cached snapshot behind observable instruments

The prior plan proposes "a periodic callback reading the existing snapshot into
an OTel meter". That is the right shape and it cannot be implemented literally,
for a reason that is one line long:

```rust
// opentelemetry-0.32.0/src/metrics/instruments/mod.rs:264
pub type Callback<T> = Box<dyn Fn(&dyn AsyncInstrument<T>) + Send + Sync>;
```

The callback is `Fn`, not `async fn`. And the snapshot is only reachable
through `ScopedRoutedGraphCluster::local_shard_runtime_metrics`
(`src/engine/cluster.rs:1238`), which is `async` — it locks the scoped-cluster
mutex at `:1239`, and per shard calls `graph_cache_entry_counts`
(`src/shard/lifecycle.rs:285`, seven `Mutex::lock().await`) and
`graph_cache_resident_bytes` (`:304`, five more). Twelve async cache-mutex
acquisitions per cell per collection, on the same mutexes the read path takes
on every cache lookup. You cannot `.await` any of that inside an OTel callback,
and `block_on` inside a callback that the SDK runs on its own OS thread
(`periodic_reader.rs:171`) is how you deadlock a node.

Note the asymmetry, because it is the design: `graph_operational_metrics`
(`src/shard/lifecycle.rs:238`) and `graph_cache_metrics` (`:234`) are
**synchronous and lock-free** — they are `AtomicU64::load(Relaxed)` over the
whole struct (`src/core/metrics.rs:168`, `:266`). It is only the two *gauge*
sets that lock.

So:

**A tokio interval task owns collection.** It runs at the export interval,
calls `local_shard_runtime_metrics().await`, and publishes the `Vec` into an
`ArcSwap` (or `RwLock<Arc<…>>`). The observable callbacks read that cached
`Arc` synchronously and report from it. The callback never blocks, never locks
a cache mutex, and never touches the runtime.

**Observable instruments, not synchronous ones.** This is not a style
preference. The source values are *cumulative* — `write_attempts` only ever
increases. An OTel `Counter::add()` takes a **delta**, so mirroring a cumulative
source through a synchronous counter means storing the last exported value and
adding the difference, which is a second copy of the state that can drift, and
which produces a spurious spike or a spurious zero at every process restart.
`ObservableCounter` reports the absolute value and lets the SDK compute
temporality. Cumulative source, observable instrument — the mapping is exact
and there is no arithmetic to get wrong.

**Duration counters become counters, not histograms.** `artifact_build_duration_us`,
`query_rows_duration_us`, `gc_duration_us` and the rest are running *sums* of
microseconds, not distributions. Exported as `ObservableCounter` alongside the
matching operation count, `rate(duration_us) / rate(count)` gives a mean, which
is what the data can honestly support. Do not export them as histograms; a
histogram built from a pre-summed total is a lie about the distribution. If
percentiles are wanted later, that is a change to the kernel's counters, not to
the export — and it should be argued on its own, because it costs a per-observation
recording where today there is one `fetch_add`.

**Interval.** 60s, matching a Prometheus scrape and keeping the twelve cache
locks per cell to once a minute. `OTEL_METRIC_EXPORT_INTERVAL` is the standard
name, consistent with the `OTEL_*`-where-OTel-defines-one rule already followed
in `crates/telemetry/src/config.rs:169-190`.

**Two hazards inherited from the prior plan's implementation notes.**

The `otlp` feature is built with `default-features = false, features =
["trace", "logs", "http-proto", "reqwest-blocking-client"]` (root
`Cargo.toml:47`) — **`metrics` is absent**, so `opentelemetry_otlp::MetricExporter`
does not currently exist in this build. One word to add, but a compile error
that will look mysterious without this note. `opentelemetry_sdk` already has
`metrics` via its defaults.

`reqwest-blocking-client` is in that list because the prior plan's Step 1 shipped
`reqwest-client` and it **panicked on the SDK's non-tokio batch threads**.
`PeriodicReader` spawns exactly such a thread (`periodic_reader.rs:171`). The
metrics exporter must use the same blocking client for the same reason, and a
staging smoke test with the endpoint actually set is the only thing that
catches it — the prior plan's own record says the `otlp`-on path was never
exercised until it broke.

### 1.6 Duplicate `/metrics`, permanently

**Recommendation: the OTel meter duplicates `/metrics`; `/metrics` is not
deprecated and not changed.**

Three reasons, in order of weight.

*`/readyz` and `/metrics` are on the same server, and one of them is a control
plane.* `graph_runtime_ready` (`admin.rs:108`) and `graph_indexer_ready`
(`graph-indexer.rs:938`) mirror the readiness probe. A pull endpoint the
cluster scrapes directly cannot depend on an external collector being up.
Anything that makes liveness observability contingent on a third-party pipeline
is a worse system, and OTLP push is by construction contingent.

*The two have different cost functions.* §1.4's `scope` divergence only works
if both exist. Collapse to one and you have to pick which cost you pay.

*Duplication is nearly free here.* Both read the same lock-free atomics. The
marginal cost of the second export is one more read of a `Vec` the interval
task already built. This is not the usual "two systems, two truths" trap,
because there is exactly one source of truth — the `AtomicU64`s — and both
exports are pure functions of it.

The one thing that must not happen is the two disagreeing about *names*. Pick
the OTel names once, write them next to the Prometheus names in the runbook,
and add a test that the meter's instrument list and `append_node_metrics`'
series list are both derived from the same enumeration of snapshot fields, so
adding a counter to `GraphOperationalMetricsSnapshot` cannot silently reach one
and not the other. That is the failure this section is most likely to produce
in a year, and a test is cheap.

**And close the sparse-kernel gap while here.** The sparse-kernel plan's Open
items note that `query_rust_sparse_fallbacks` "only separates compiled from
uncompiled", so the Cypher path cannot distinguish kernel 2 from kernel 3.
`turbolay.kernel` is already on the `kernel.expand` span
(`src/shard/query.rs:24`) and is on the safe-label list. A
`turbolay.query.traversals{kernel="…"}` observable counter answers it directly,
and it is the one place in this document where the metric is strictly better
than the span — "which rung is serving traffic" is a rate question, and reading
it off sampled traces at 5% is guessing.

### 1.7 Exemplars: not available, and not only because of the SDK

An exemplar attaches a trace id to a metric data point, so a spike in a latency
series links to one trace that contributed to it. It is the right idea and it
cannot be built today.

**The SDK does not populate them.** `opentelemetry_sdk` 0.32.1 has the data
model — `Exemplar<T>`, and `exemplars()` accessors on every data-point type
(`src/metrics/data/mod.rs:174`, `:233`, `:358`, `:483`) — and every aggregator
constructs its data points with `exemplars: vec![]`
(`src/metrics/internal/aggregate.rs:271`, `:303`, `:308`, `:350`, `:355`,
`:401`, `:458`). There is no reservoir, no filter, and no code path anywhere in
`src/metrics/internal/` that pushes one. Verified by reading the vendored
source, not by reading the changelog.

**And the sampler would undercut them anyway.** An exemplar is a pointer to a
trace. `crates/telemetry/src/sampling.rs` head-samples at 5%, and its own
module docs (`:1-31`) explain that keeping error traces is a *tail* decision
belonging in the collector. So an exemplar recorded on a slow-query data point
would, 95% of the time, point at a trace that was dropped at head — a link that
404s. Making exemplars useful means near-100% head sampling plus collector-side
tail sampling with the exemplar's trace id in the keep policy, which is a real
deployment design and not a checkbox.

**Recommendation: do not pursue exemplars.** Revisit when the SDK implements
them *and* the sampling story from §3's baseline week is settled. Until then,
`turbolay.query.full_scan` as a label on the metric and as an attribute on the
span gives the same navigation — see the spike, filter the traces on the same
attribute — without a pointer that can dangle.

### 1.8 Sequencing

**Step M1 — the meter provider and the operational counters.** Add
`"metrics"` to `opentelemetry-otlp` in the root `Cargo.toml`; add
`SdkMeterProvider` to `otlp::Providers` (`crates/telemetry/src/otlp.rs:43`) and
to its `shutdown` (`:50`); add `MetricLabel`, `METRIC_LABELS`, `SPAN_ONLY_KEYS`
and the partition test to `semconv.rs`; add the interval task and the
`ObservableCounter` set for `GraphOperationalMetricsSnapshot` and
`ClientQueryMetricsSnapshot`, labelled by `cell_id` only.

**Done when** `write_retries`, `query_rows_failed` and `verifier_failures` —
none of which have ever left a process — are charted per cell in staging, and
`/metrics` returns byte-identical output to before.

**Step M2 — cache counters and gauges.** The 19 `GraphCacheMetricsSnapshot`
counters and the two gauge structs. Separate because it is the step that pays
the twelve-lock collection cost, and it should be measured against read-path
latency before it is turned on everywhere.

**Done when** matrix-artifact hit rate is a series, and the p99 of
`query.execute` in staging is unchanged with the collector on.

**Step M3 — the indexer.** `IndexerMetrics` through the same meter, with
`generations_published` / `generation_failures` / `generations_deleted`
dimensioned by `cell_id` and `edge_type`.

**Done when** "which cell's index is failing" is answerable from a metric
rather than only from a trace.

## 2. Span links from the write path to the indexing path

The prior plan adopted attribute-based correlation — `cell_id` plus
`generation` / `base_sequence` / `read_epoch` on all three paths — and deferred
span links because stamping a trace id into artifact metadata changes a
persisted storage format. That deferral was correct. Having now read the
format, it should be **deferred indefinitely**, and the reason is stronger than
"it changes a format".

### What the format change would actually be

There are two persisted manifests, both tab-separated single lines with a magic
first field and a strict field count.

`GraphIndexGeneration` — `src/engine/index_store.rs:11`, encoded at `:350`:

```
turbolay-index-current-v1 \t cell_id \t edge_type \t base_sequence \t
last_wal_id \t edge_count \t checksum \t generation \n
```

`decode_graph_index_manifest` (`:364`) rejects anything else:

```rust
if fields.len() != 8 || fields[0] != INDEX_MANIFEST_MAGIC {
    return corrupt(key, "expected turbolay index current v1 manifest");
}
```

`MatrixArtifact` — `src/engine.rs:1213` / `:1227` — is the same pattern with
`matrix_manifest1` and `parts.len() != 8`.

So a traceparent field is a ninth field, and a ninth field is `CorruptValue` on
every reader that has not been upgraded. Not a degraded read, not a missing
attribute — a hard error out of `current_graph_index`
(`src/engine/index_store.rs:48`), which propagates rather than falling back.
An indexer deployed ahead of the fleet would make every graph-node fail to
read the manifest it just wrote.

The migration is therefore two releases, in this order: relax both decoders to
accept `>= 8` fields and ignore trailing ones, ship that everywhere, confirm,
*then* start writing nine. Two coordinated deploys, for telemetry.

### The part that makes it worse than a two-release migration

The generation identity **is** the content hash:

```rust
// src/engine/index_store.rs:127-129
let checksum = graphblas_csc_checksum(&csc);
let payload = encode_graph_index_csc(base_sequence, last_wal_id, checksum, &csc);
let generation = sha256_hex(&payload);
```

`publish_graph_index` (`:236`) relies on that: it writes the generation object
with `PutMode::Create` and treats `AlreadyExists` as success (`:254-260`),
because identical content produces an identical path. Put a trace id anywhere
near the payload and identical graph content produces different digests, which
breaks content-addressed dedup, breaks the idempotent republish, and makes
`generation` stop being a value two nodes can compare.

The manifest is a separate object from the payload, so a *manifest-only* field
would not touch the digest. But that is a narrow escape from a self-inflicted
problem, and it should be stated plainly in case somebody later reaches for
"just put it in the CSC header, it's already got a version magic".

### What a span link would buy

Honestly: **very little, today.**

The attribute join already shipping answers the question that is actually
asked. `cell_id` and `generation`/`base_sequence` are on the indexer's
`artifact.build`, `artifact.publish` and `artifact.gc` spans and on the read
path's artifact lookup, so "every span touching cell-7 at generation 412" is
one backend query across two services. The three bugs this correlation exists
for — BFG-006, BFG-013, BFG-014 — are all of the form "did these three agree on
a generation", and that is an equality question about a value both sides
already carry.

A span link would add three things:

1. **One clickable edge** instead of a query. Genuine, and genuinely minor.
2. **Survives clock skew.** The attribute join does not depend on timestamps
   either, so this is not the advantage it sounds like.
3. **Disambiguates concurrent writers.** This is the only real one. If two
   nodes wrote to `cell-7` and the indexer compiled one generation from both,
   the attribute join returns both writes and cannot say which one the build
   was triggered by. A link points at exactly one.

Set against a two-release manifest migration and a permanent extra field in a
storage format, (3) is not worth it — especially since "two nodes wrote to
cell-7" is itself the ping-pong incident, which §3 detects directly and which
should be *fixed* rather than made more traceable.

**Recommendation: do not ship span links. Revisit only if a concrete incident
is analysed and the analysis fails specifically because the attribute join
returned an ambiguous set.** That is a falsifiable trigger, which is the point
of writing it down; "revisit if it proves insufficient" is not.

There is one cheap thing worth doing in the meantime. Both decoders should be
relaxed to accept trailing fields **now**, as a standalone change, entirely
independent of telemetry. Right now neither manifest can gain a field without a
coordinated two-release deploy, which is a general schema-evolution problem
that will bite the first time somebody wants to add something load-bearing.
Doing it now costs four lines and removes the largest part of the objection
above, which is the honest way to keep this option open.

## 3. Trace-driven alerting

The prior plan's instruction — ship, watch a week of staging, write alerts
against what is actually noisy — stands. This section is what to watch *for*,
and what each candidate needs before it can be an alert rather than a chart.

### The rule that governs all of them

`contention` and `fencing` are **expected**.
`ErrorClass::is_expected_under_contention`
(`crates/telemetry/src/error_class.rs:86`) already encodes this, and
`refresh_writer_fence` records `error.class = fencing` on a path that runs once
per write (`src/core/state.rs:441`). Retries are how the system is supposed to
behave under concurrency. An alert on the *occurrence* of either class will
fire on day one, be silenced by day three, and take the credibility of every
other alert in this document with it.

So: chart the rate, alert on a **change** in the rate. Every candidate below is
phrased that way, and the baseline is what the staging week is for.

### Candidate 1 — writer ping-pong (strongest)

**Signal.** Group `writer.fence_refresh` spans by `turbolay.cell_id`; count
distinct `turbolay.writer.last_promoted_by` over a five-minute window. A count
above one *is* the duel — the `cell-writer-fencing-pingpong` incident stated as
a query. The mechanism is already in place: `log_fence_attribution`
(`src/core/state.rs:455`) reads the advisory cell-writer record and the three
fields are promoted onto the enclosing span.

**The trap, and it is specific.** The three `last_promoted_*` attributes are
recorded **only in the fence arm** (`src/core/state.rs:426-442`). Every other
`writer.fence_refresh` span — the overwhelming majority, one per write across
29 `acquire_local_write_guard` call sites — has them empty. An alert written
against "fence refresh spans" instead of "fence refresh spans with
`last_promoted_by` set" is measuring write throughput.

**Second trap: sampling.** With no client parent the span is always sampled
(`sampling.rs:43`); under a `client.mutate` root the parent decides at 5%
(`:115-130`). A distinct-count over a 5% sample systematically *under*-reports
distinct values. Either force full sampling for spans carrying
`last_promoted_by`, or accept that the alert detects sustained ping-pong and
not a single exchange — and write which one it is into the alert description.

**Baseline needed.** Fence events per cell per hour under normal operation,
and how many of those carry a `last_promoted_by` that differs from the local
`node_id`. The expected steady state is a small nonzero number from ordinary
rebalances; the incident is a sustained count of three or more distinct
promoters on one cell. That threshold is a guess until the week of data exists.

**Why this is the strongest candidate.** It is the only one where the alert
condition corresponds to a known production incident with a known cause and no
current detection at all.

### Candidate 2 — full-scanning queries

**Signal.** Rate of `query.plan` spans with `turbolay.query.full_scan = true`,
by `turbolay.cell_id`. Set at `src/shard/query_optimizer.rs:959` from the plan
shape alone, regardless of elapsed time (`:953-981`).

**Alert on the derivative, twice over.** A new full-scanning shape appearing —
a `full_scan` rate that was zero and is not — is the actionable event, because
it means a deploy changed a query or a plan flipped. A *rising* rate on an
existing shape means a tenant grew into the problem. Both are changes; neither
is a threshold on the absolute number, because some applications legitimately
full-scan small collections and will do so forever.

**Sampling is handled.** `RowQueryPlanSummary::record` sets
`turbolay.sampling.force` when `full_scan` is true
(`src/shard/query_optimizer.rs:960-963`), and the head sampler honours it
(`sampling.rs:72-74`). Full-scan spans are kept at 100%, so this rate is exact
rather than scaled — which is worth knowing, because it means candidate 2 and
candidate 1 need *different* correction factors and mixing them on one
dashboard will mislead.

**Better as a metric than an alert.** §1.3 puts `query.full_scan` on the safe-label
list precisely so `rate(query_rows_started{full_scan="true"})` exists. Alert on
the metric, use the trace to find out which query. That is the division of
labour this whole document is arguing for.

**Baseline needed.** The set of distinct `turbolay.query.fingerprint` values
that full-scan at all today. If that set is small, the alert is "a fingerprint
outside the known set full-scanned" and it is excellent. If it is large, the
first job is fixing plans, not writing alerts.

### Candidate 3 — indexer readiness flapping

**Signal.** `index.cycle` spans with `turbolay.outcome = failed`
(`src/bin/graph-indexer.rs:313`), and the readiness-transition events at `:327`
and `:301`. The transition is recorded on the flip only, not the steady state —
`ready` is tracked alongside the atomic (`:266`) exactly so the event means a
transition.

**Alert on transition count, not on state.** An indexer that is unready and
staying unready is caught by the Kubernetes probe (`indexer_readiness`, `:927`)
and needs no second alarm. An indexer *flapping* — ready, unready, ready — is
not caught by anything and is the failure mode where one bad cell out of eight
makes a whole pod cycle. More than two transitions in fifteen minutes is a
reasonable starting shape.

**Baseline needed.** Transitions per day in steady state, which should be zero
and probably is not.

### Candidate 4 — freshness

**Signal.** `error.class = freshness` — `SnapshotAhead`, `SnapshotExpired`,
`SnapshotChanged`, `QueryStatsSnapshotChanged`, `ControlWatermarkRegression`.

Unlike `contention` and `fencing`, `freshness` is **not** routine. It is the
BFG-007 / BFG-009 / BFG-011 family, and each one is a genuine disagreement
about what a reader should have seen. This is the one class where a low
absolute threshold is defensible.

**Trap.** Some freshness errors are retried transparently and never reach a
client, so the rate is not a user-impact rate. Pair it with
`query.bookmark_wait` duration, which *is* read-your-writes latency as felt.

**Baseline needed.** Whether the steady-state rate is genuinely zero. If it is,
alert on any occurrence. If it is not, the first question is which of the five
variants is firing and whether that is a bug rather than an alerting problem.

### Not candidates

`query.admission` rejections and `QueryTimeout` are admission control working;
they belong on a capacity dashboard. `placement.resolve` with
`ownership = remote` is normal routing (`src/engine/cluster.rs:528-551`
deliberately records all four values so the rate has a denominator) — chart the
*proportion*, do not alert on it.

## 4. Continuous profiling and span metrics

Both are reasonable and neither is the current gap. Briefly, so that "we
considered it" is on the record.

**Continuous profiling** answers "which function is burning CPU". Turbolay's
open questions are not shaped that way. The sparse-kernel plan's outstanding
benchmark question — kernel 3's `mxv` advantage over kernel 2, unmeasured since
the threading fix — is the closest thing to a profiling question in the repo,
and it is a benchmark, not a production profile. Meanwhile the actual
production incidents (writer ping-pong, full-scanning plans, generation
disagreement) are all *coordination* failures, where the process is idle and
waiting or doing the wrong work efficiently. A profiler shows neither.

**Span metrics** — deriving RED metrics from span data in the collector — is a
real technique and is the wrong one here, for a reason specific to what §1
found. Span metrics are valuable when the only telemetry is traces. Turbolay
has 65 hand-maintained counters that are already correct, already cheap, and
already cover the write path, the artifact pipeline and the caches — none of
which is currently exported. Deriving a worse version of a subset of them from
5%-sampled spans, while the exact versions sit unexported in an atomic, is
strictly the wrong order of work.

Revisit span metrics if, after Steps M1–M3, there is a rate question the
counters cannot answer. The likeliest candidate is per-phase read-path latency
(`query.parse` versus `query.plan` versus `query.execute`), which exists only
as spans. If that turns out to be the recurring question, the right fix is
probably a counter, not a collector processor.

## 5. Open decisions

**Whether the cache gauges are worth their locks.** `graph_cache_entry_counts`
and `graph_cache_resident_bytes` take twelve async mutex acquisitions per cell
per collection (`src/shard/lifecycle.rs:285-320`), on mutexes the read path
uses. `/metrics` already pays this per scrape and nobody has complained, which
is weak evidence it is fine. **Settled by:** measuring `query.execute` p99 in
staging with M2 on and off at a 60s interval and at 10s. If 10s is visible in
p99, the gauges need their own slower interval, or `resident_bytes` needs to be
maintained incrementally rather than computed on read.

**Whether `turbolay.query.fingerprint` may ever be a metric label.** It is the
one attribute whose safety depends on the workload rather than on the schema.
A `query_rows_duration_us` broken down by fingerprint would be genuinely
excellent for exactly the "this shape got slower" question the fingerprint was
built for. **Settled by:** counting distinct fingerprints per scope over the
staging week. Under a few hundred, it is worth an opt-in flag with a hard cap
and an overflow bucket. Above that, it stays span-only forever.

**Whether `error.class` should be a label on a dedicated error counter.** It is
on the safe list at 11 values, but there is no error counter today to put it on
— `query_rows_failed` is undimensioned. Adding one means a kernel change
(`GraphError::class()` already exists, so the mapping is free) and a new atomic
per class. **Settled by:** whether the staging week shows `error.class` being
read off logs frequently enough to justify a counter. If every investigation
starts with a log query grouped by class, that is the signal.

**The metric naming scheme.** `turbolay.*` matching the span registry, or
`graph_*` matching the existing Prometheus names, or OTel-idiomatic
`db.client.*` where a semantic convention exists. Leaning toward `turbolay.*`
for anything with no semconv equivalent, because one vocabulary across spans
and metrics is what makes a dashboard and a trace view feel like one system.
**Settled by:** checking whether the backend's out-of-the-box database
dashboards key off `db.*` metric names the way the APM view keys off
`db.system.name` — if they do, the semconv names earn their inconsistency.

## 6. Explicitly out of scope

- **Changing `/metrics`.** Same conclusion as the prior plan, now with the
  additional reason in §1.4: the two exports should be allowed to differ in
  dimensionality.
- **Adding counters to the kernel.** §1 exports what exists. Where a counter is
  missing — a per-rung traversal count, a per-class error count — it is named
  as such and left to its own change.
- **Fixing anything the metrics reveal.** Unchanged from the prior plan.
- **Exemplars.** §1.7, and unlike the prior plan's version of this line, with a
  verified reason rather than a deferral.
- **The `/metrics` `scope` label.** Left as it is, deliberately, and documented
  rather than fixed.
