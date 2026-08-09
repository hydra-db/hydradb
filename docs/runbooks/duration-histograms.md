---
title: Duration histograms — what they answer and what they do not
kind: runbook
status: living
branch: Turbolay-V3.5
written_against: 1d66650
last_reviewed: 2026-07-27
plan: docs/plans/2026-07-26-otel-metrics-span-links-and-alerting.md
tags:
  - metrics
  - prometheus
  - otlp
  - latency
  - runbook
---

# Duration histograms

Turbolay records latency distributions as fixed-bucket histograms in the kernel
and exposes them on `graph-node`'s `/metrics` endpoint and, when OTLP is
configured, through the meter. This document is step H3 of §1.10 of the metrics
plan. It is here because every failure mode of a fixed-bucket histogram is a
*misuse* failure, and the misuses are predictable: a wrong quantile query returns
a plausible number rather than an error, and a wrongly phrased alert fires
forever on arithmetic rather than on a regression.

Read this before building a dashboard panel or an alert rule on any series named
below. It is a living document — no date in the filename — and it was written
against `1d66650`. Everything factual in it was read out of the code at that
commit; where the plan and the code disagreed, the code won, and the disagreement
is recorded in "Where this differs from the plan" at the end.

## The ladder

One ladder, one constant, shared by both exports:
`slatedb_graph_kernel::DURATION_BUCKET_BOUNDS_US` in `src/core/histogram.rs`.
Seventeen finite upper bounds in microseconds, plus a `+Inf` overflow, so
eighteen buckets:

```
100, 250, 500, 1_000, 2_500, 5_000, 10_000, 25_000, 50_000,
100_000, 250_000, 500_000, 1_000_000, 2_500_000, 5_000_000, 10_000_000, 30_000_000
```

Rendered as `le` label values that are microseconds for three of the Prometheus
families and seconds for two — and, because OTLP folds client read and write into
one instrument, seconds for exactly one OTel instrument, which is what
`HistogramUnit::Seconds` in `crates/telemetry/src/meter.rs` means when it says so:
`100` … `30000000`, and `0.0001, 0.00025, 0.0005, 0.001,
0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30`. Both
exports render the bound from the same constant through the same code
(`ExportUnit::render_bound` in `src/bin/graph_node/otel_metrics.rs` and
`HistogramUnit` in `crates/telemetry/src/meter.rs`, character-for-character
identical by intent), because two spellings of one bound are two series to
everything downstream.

Three rungs are fixed by code rather than by taste, and moving them breaks a
cross-check rather than a preference. **500 ms** is `slow_query_log_threshold`;
the mass above that bound and the `slow_queries` counter are the same event.
**30 s** is both `DEFAULT_MAX_QUERY_RUNTIME_MS` and
`DEFAULT_QUERY_TRANSPORT_TIMEOUT_MS`, so the overflow bucket means "timed out",
not "slow". **100 µs** is the floor because nothing user-facing in an
object-store-backed graph completes below it except a pure cache hit.

Successive rungs are 2× or 2.5×, except the last, which is 3× — the 10 s to 30 s
band, which is exactly where timeouts land. That ratio is the source of every
accuracy limit below.

## The families

| Prometheus series stem | Labels | Unit | Kernel field | Live at `1d66650`? |
| --- | --- | --- | --- | --- |
| `graph_client_operation_read_duration_seconds` | none | s | `read_latency` | yes |
| `graph_client_operation_write_duration_seconds` | none | s | `write_latency` | yes |
| `graph_query_rows_duration_microseconds` | `scope`, `cell_id` | µs | `query_rows_latency` | yes |
| `graph_query_transport_rpc_duration_microseconds` | none | µs | `rpc_latency` | **no source** |
| `graph_query_transport_serve_duration_microseconds` | none | µs | `serve_latency` | **no source** |

Each stem yields `…_bucket{…,le="…"}`, `…_sum` and `…_count`, which is the shape
`histogram_quantile` expects. The name table is `PROMETHEUS_HISTOGRAMS` in
`src/bin/graph_node/admin.rs`; the OTel table is `OTEL_HISTOGRAMS` in
`src/bin/graph_node/otel_metrics.rs`; a field in one and not the other is a build
failure (`every_histogram_field_reaches_both_exports`).

The two transport families render correctly and nothing feeds them.
`graph-node` instantiates neither `TcpQueryServer` nor `TcpQueryCellClient`, so
it never holds a `QueryTransportMetricsSnapshot`; the export covers the fields
because they live in the kernel and the export must not be the thing that decides
which binary is allowed to have a metric. On a `graph-node` scrape they are all
zeros with a `_count` of 0. That is not a broken exporter and there is nothing to
fix on the metrics side.

Client read and write are two Prometheus names. In OTLP they are one instrument,
`db.client.operation.duration`, told apart by `db.operation.name`. The shard
family is `turbolay.query.rows.duration` in OTLP and carries `cell_id` only —
never `scope`. That divergence between the two exports is deliberate and is the
reason both exist.

## Reading a quantile

The p99 of client reads across the fleet, over five minutes:

```promql
histogram_quantile(
  0.99,
  sum by (le) (rate(graph_client_operation_read_duration_seconds_bucket[5m]))
)
```

The same thing done wrong:

```promql
# WRONG. Do not use.
avg(
  histogram_quantile(
    0.99,
    rate(graph_client_operation_read_duration_seconds_bucket[5m])
  )
)
```

The second form computes one p99 per node — `histogram_quantile` groups by every
label except `le`, and `instance` is one of them — and then averages those
numbers. The average of quantiles is not the quantile of the union. It is not an
approximation of it either; it is a different quantity with no operational
meaning, and it moves in the wrong direction when one node's traffic share
changes while nothing about latency does. Summing the bucket families first and
taking the quantile of the sum is the only correct order. Nothing in the pipeline
prevents the wrong one: both queries return a float, both plot, neither warns.
This is dashboard discipline or it is nothing.

The shard family carries `scope` and `cell_id`, so its aggregation has to say so.
Fleet-wide:

```promql
histogram_quantile(
  0.99,
  sum by (le) (rate(graph_query_rows_duration_microseconds_bucket[5m]))
)
```

Per cell, which is the useful cut when one cell is the suspect:

```promql
histogram_quantile(
  0.99,
  sum by (cell_id, le) (rate(graph_query_rows_duration_microseconds_bucket[5m]))
)
```

Both return **microseconds**. Divide by 1000 for milliseconds. The unit is in
the series name precisely so that this is a choice rather than a guess; a series
whose unit has to be inferred is a series read off by a factor of a million.

The mass above the slow-query threshold, which is what the 500 ms rung exists
for:

```promql
sum(rate(graph_query_rows_duration_microseconds_bucket{le="+Inf"}[5m]))
  - sum(rate(graph_query_rows_duration_microseconds_bucket{le="500000"}[5m]))
```

## What the numbers do not support

### The quantile is an estimate

`histogram_quantile` interpolates linearly inside the bucket the quantile falls
in. It has no information about where inside that bucket the observations
actually sit, so the error is bounded by the bucket's width: worst case, about
(rung ratio − 1) in relative terms. This ladder's rungs are 2× and 2.5×, with a
3× at the top, so the worst case is large — a p99 reported as 700 ms could be
anywhere in the 500 ms to 1 s band. In practice, with mass spread through the
bucket rather than piled at one end, the estimate lands within a few tens of
percent.

What that buys is real and what it does not buy is also real. It answers **"is
p99 10 ms or 100 ms"** — those are five rungs apart and no interpolation error
crosses that. It answers **"did p99 move by 2×"** — a 2× move relocates mass
across a rung, and the bucket counts show that directly whether or not the
interpolated number is exact. It does **not** answer **"did p99 go from 42 ms to
47 ms"**. Both of those values are inside the 25 ms–50 ms bucket, and the
difference between the two reported numbers is a change in how much mass sits in
that bucket relative to its neighbours, not a measurement of 5 ms. An alert
phrased on a small percentage change in a reported quantile is measuring the
interpolation, and it will be noise.

### There is no per-tenant and no per-fingerprint breakdown

This is the first thing an operator asks for, and the answer is no. Not "not
yet" — no.

`turbolay.query.fingerprint` is span-only, unconditionally. It is minted
*before* request validation runs, so any authenticated client can mint an
unbounded set of fingerprints out of strings that are not valid queries and never
execute. That makes it attacker-reachable, not merely workload-dependent, and a
cardinality bound an authenticated caller can violate on purpose is not a bound.
A second, independent reason: the normaliser is byte-level rather than AST-level,
so `MATCH (n:Person)` and `MATCH (x:Person)` are different fingerprints, and so
are `match` and `MATCH` — even a well-behaved client produces one series per
*spelling*. The metric-label registry in `crates/telemetry/src/semconv.rs` makes
adding it a type error rather than a review comment. See §5.2 of the plan.

Per-tenant latency is likewise not available from the histograms in the sense
that matters. `graph_query_rows_duration_microseconds` does carry `scope` — it
inherited that from the counters it sits beside — but that is one of a fixed list
of six legacy families and nothing new may join them, and the client-side
families carry no scope at all, so end-to-end latency per tenant is not there and
will not be. **"Which tenant got slow" is a trace question.** Answer it from
spans, where the fingerprint and the scope both live.

### `_sum` can lag the buckets by the in-flight concurrency

`AtomicDurationHistogram::record_micros` performs two independent `Relaxed`
`fetch_add`s — the bucket first, then the sum. They are deliberately not atomic
with respect to each other; the alternative is a lock on the query path. A
snapshot taken between the two sees the observation counted in its bucket and not
yet in the sum, so `_sum` can undercount by at most the observations in flight at
that instant. That bound is small and the sum is cumulative, so it is invisible
at any window a dashboard uses; the reason to know it is that
`rate(_sum) / rate(_count)` can be a hair low, and a mean derived that way is not
a witness against the buckets.

`_count` and `le="+Inf"` never skew, by construction and not by luck.
`DurationHistogramSnapshot` has no `count` field: `count()` sums the buckets, and
both the `_count` line and the `+Inf` bucket are rendered from that one number.
If they ever disagree, the rendering is broken, not the recording.

### Falling and vanishing series

Two distinct behaviours, and the failure modes differ.

The **per-cell counter** families — everything labelled `cell_id` and not
`scope`, which is most of `/metrics` since M2/M3 — are **summed over every scope
open on the node**. A scope closing therefore makes the sum *fall*, and `rate()`
reads a falling counter as a reset and undercounts across it. This is the
documented cost of the shape, and it is worth understanding why the shape is not
negotiable: the alternative is a `scope` label, and `scope` is the unbounded
tenant root, so a node holding *T* scopes would multiply every one of these
families by *T*. Worse, emitting one series per `(scope, cell)` instead of
summing is not merely expensive — two scopes hosting the same `cell_id` would
render the same series name and label set twice with different values in a single
scrape, and Prometheus rejects the **entire** response rather than one series.
Undercounting a rate at a scope eviction is the cheaper of the two failures.
`PrometheusCounterExport::PerCell` in `admin.rs` carries this argument in its doc
comment ("summed over every scope open on the node"), and
`per_cell_counters_are_summed_across_scopes` pins it.

The **histogram** families behave differently and the difference is in your
favour. The client and transport families are process-global and reset only on
process restart. The shard family carries `scope`, so it is never summed across
scopes, so it never falls partway: the metrics come from currently-open shards
(`local_shard_runtime_metrics`), so a cell that is closed simply stops reporting
and its series goes stale, and a cell that is reopened starts its buckets from
zero. `rate()` handles a reset to zero correctly. An `increase()` over a window
straddling a reopen still undercounts, so prefer `rate()` and treat a cell that
was evicted mid-window as missing data rather than as a drop in traffic.

## `error_class`, not `error.class`

The error-class breakdowns are not histograms, but they are the first registry
key that cannot be spelled the same in both exports, and it will be read as a
typo if it is not written down.

A Prometheus label name must match `[a-zA-Z_][a-zA-Z0-9_]*`. A dot is a parse
error, not a stylistic disagreement. So the registry key `error.class` is spelled
`error_class` on `/metrics` and `error.class` in OTLP. The constant is
`ERROR_CLASS_LABEL` in `src/bin/graph_node/admin.rs`, which exists so that the
divergence is written down in one place rather than discovered from a rejected
scrape. The series:

```
graph_query_failed_by_class{error_class="…"}
graph_query_rows_failed_by_class{cell_id="…",error_class="…"}
```

Twelve values, from `GraphError::CLASSES`: `contention`, `fencing`, `routing`,
`freshness`, `admission`, `timeout`, `query`, `authz`, `corruption`, `config`,
`storage`, `kernel`. `routing` counts wrong-node and unavailable-route outcomes;
`fencing` is reserved for writer lifecycle failures, including SlateDB's fenced
close reason. The per-class array is total by construction against the scalar `query_rows_failed`
counter, because the same call increments both, so one can be used to check the
other.

## The OTLP side: two deliberate non-conformances

Both of these will be read as bugs by anyone who does not find them here. Neither
is.

**`db.client.operation.duration` is emitted without `db.namespace`.** Semantic
conventions mark it required-if-applicable, and it is applicable — the namespace
is the `scope`. It is omitted because `scope` is the unbounded tenant root the
whole label registry exists to keep off metrics. The instrument does carry
`db.system.name`, which is the attribute a vendor's database view keys on. A
conformance checker will flag the missing attribute; the flag is the design. See
§1.9 and §5.4.

**The buckets reach OTLP as observable counters, not as a histogram data point.**
`opentelemetry` 0.32 has `u64_observable_counter`,
`f64_observable_up_down_counter` and `u64_observable_gauge`, and for histograms
only the *synchronous* `f64_histogram` / `u64_histogram`; `opentelemetry_sdk` 0.32
has no `MetricProducer`. There is no observable histogram, so a distribution
computed in the kernel and read from a cached snapshot cannot leave as a histogram
data point. It leaves as `<name>.bucket` carrying an `le` attribute — one series
per bucket — plus `<name>.sum` and `<name>.count`, which is precisely what a
Prometheus histogram already is. The consequence is that **vendor-native latency
widgets will not light up on this data.** The semconv name and `db.system.name`
still put the series in the right view; the p99 panel that view offers will not
populate, and you will build the panel from the bucket family by hand. The
compatibility §1.9 bought is real but partial, and this is the half that is not
delivered.

The synchronous OTel `Histogram` was not an option and it is worth knowing why,
because "just use the SDK's histogram" is the obvious suggestion.
`ValueMap::measure` takes an `RwLock` read, hashes a `Vec<KeyValue>` for a map
lookup, then takes a `std::sync::Mutex` per attribute set — a lock, a hash and an
allocation per observation, in the query path, against the one relaxed
`fetch_add` this codebase pays everywhere else. It would also be a kernel
dependency on `opentelemetry`, which `crates/telemetry`'s manifest forbids.

## Revisit trigger

The ladder and the whole fixed-bucket approach are wrong, and should be replaced
with a relative-error sketch, the first time either of these happens:

1. An alert of the form "p99 rose more than X% week-over-week" **fires on
   bucket-boundary noise** — mass shifting inside one bucket, with no real change
   in latency.
2. The same alert **fails to fire on a real regression that stayed inside one
   bucket**.

Either one is a falsification of "a 2–2.5× ladder is enough resolution for the
questions we ask", and it should be treated as such rather than argued with.

Secondary trigger, and it is a re-cut rather than a replacement: if any histogram
shows **90% or more of its mass in two adjacent buckets**, re-cut the ladder
first. It is one constant, in one file, shared by both exports, and a ladder
concentrated in two rungs is a ladder cut for the wrong workload — not evidence
that fixed buckets cannot work. Reach for a sketch only after a re-cut has been
tried and has not helped.

Check the secondary condition by reading the *cumulative fraction* of mass at
each bound over a long window, on a table panel:

```promql
sum by (le) (increase(graph_query_rows_duration_microseconds_bucket[1w]))
  / scalar(sum(increase(graph_query_rows_duration_microseconds_bucket{le="+Inf"}[1w])))
```

The buckets are cumulative, so this is a monotonic column from near 0 to exactly
1. The condition is met when the column jumps from below about 0.05 to above
about 0.95 across two consecutive rows: everything is in those two rungs and
every other rung is carrying nothing.

## Known gaps at `1d66650`

None of these is a defect in the histogram design; all three change what an
operator can actually query today.

- **`rpc_latency` and `serve_latency` have no source in `graph-node`.** Covered
  above. Both families scrape as zeros.
- **`slow_queries` is not exported anywhere.** It lives on
  `QueryTransportMetricsSnapshot`, which `graph-node` never holds, and
  `CounterSource` has no `Transport` variant, so no name table covers the
  transport counters at all. The 500 ms rung was pinned so the mass above it
  could be reconciled against `slow_queries`; that reconciliation is currently
  not performable on a `graph-node` scrape. The query above is still the right
  one — there is just nothing to check it against yet.
- **Counters reach the meter for one snapshot only.** All 67 enumerated counters
  reach `/metrics`. As of `2a5a8d1` the 34 registrable operational (shard)
  scalars also reach the meter, summed by `cell_id` across scopes;
  `ClientQueryMetricsSnapshot` (10), `GraphCacheMetricsSnapshot` (19) and the two
  per-`error.class` breakdowns do not, the last pending a `cell_id × error.class`
  cardinality decision. `METERED_COUNTER_SOURCES` is where that scope is
  declared, and `only_the_shard_scalars_are_registered_this_round` pins it, so
  the next round is a decision rather than a discovery. Histograms reach both
  exports in full.

## Where this differs from the plan

Three things in §1.10 of
`docs/plans/2026-07-26-otel-metrics-span-links-and-alerting.md` do not match the
tree at `1d66650`. The plan is the orchestrator's to amend; they are recorded
here so this document is not read against it and assumed wrong.

1. **"20–30% in practice" understates the worst case.** §1.10 and Step H3 both
   give the worst-case relative error as "about (bucket ratio − 1), which for
   this ladder is 20–30%". Those two clauses are inconsistent: the shipped
   ladder's rungs are 2× and 2.5× (and one 3×), so (ratio − 1) is 100–150%, not
   20–30%. The bound and the typical case have been separated above. The
   operational conclusions — 10 ms versus 100 ms yes, 2× yes, 42 ms versus 47 ms
   no — are unaffected, which is presumably why the discrepancy survived.
2. **The shard histogram is not summed across scopes.** Step H3's fourth bullet
   and §1.4's shape argument apply to the per-cell *counter* families.
   `graph_query_rows_duration_microseconds` carries `{scope, cell_id}` — commit
   `8d7e939` gave it the same labels as the counters it sits beside — so it is
   the sixth member of the fixed scope-labelled list, and it goes stale rather
   than falling when a scope closes. `only_the_pre_existing_families_carry_a_scope_label`
   in `admin.rs` pins the list at six and names the divergence.
3. **The type is not where §1.10 says.** It is `src/core/histogram.rs`, not
   `src/core/metrics.rs`, and the recorder is `AtomicDurationHistogram`
   (`pub(crate)`) with only `DurationHistogramSnapshot`,
   `DURATION_BUCKET_BOUNDS_US` and `DURATION_BUCKET_COUNT` public. The plan's
   H1 divergence list already records this; it is repeated here because a reader
   grepping `metrics.rs` for the ladder will not find it.
