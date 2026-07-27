---
title: OTel metrics, span links and trace-driven alerting
status: draft-for-review
date: 2026-07-26
branch: Turbolay-V3.5
base_commit: 6255ca3
tags:
  - observability
  - opentelemetry
  - metrics
  - cardinality
  - alerting
  - histograms
---

# OTel metrics, span links and trace-driven alerting

**Amended 2026-07-27 against `6255ca3`.** The original was written against
`02eba8c`, which predates two commits of this plan's own lineage (`93b5077`,
`6255ca3`); `base_commit` above is corrected accordingly. The amendment does
four things: it settles all four of §5's open decisions from code rather than
from a staging week, it records three live bugs a second pass through the same
files turned up, it corrects a structural claim in §1.5 that does not survive
contact with the OpenTelemetry metrics API, and it adds §1.9 (the naming
decision) and §1.10 (percentiles, H1–H3) — the latter being what §1.5's
"if percentiles are wanted later" was pointing at. Everything the second pass
falsified is listed in "What the code audit falsified" below and also corrected
in place; nothing that still holds has been removed.

**Reconciled 2026-07-27 against `8f150b2`.** The amendment above was committed
(`f9a4aaf`) before the BUG-3 fix landed. The fix split `turbolay.sampling.force`
into a head key and a new tail key, and driving the real sampling path corrected
two things this document had asserted from a code read. BUG-3's entry below now
records what actually shipped; §1.3, §1.7, §1.8 and §3's candidates 1 and 2 are
corrected in place. The short version: the head-sampling decision is taken at a
span's **first entry**, not at creation, and full-scan spans are still
ratio-sampled — by design, and now waiting on a collector policy rather than on
code.

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
- `src/engine/cluster.rs:1265` and `src/shard/lifecycle.rs:234-330` — the
  collection path, and the reason half of it is lock-free and half is not.
- `src/engine/index_store.rs` — `GraphIndexGeneration` (:12),
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

**Read for the 2026-07-27 amendment, and not read the first time**

The four §5 decisions were all supposed to need a staging week. None of them
did; each was settled by reading a file this plan had not opened. Those files
are named here so the next reader can check the answers rather than re-derive
them.

- `src/client/service.rs:1570` (`validate_request`), `:1900`
  (`record_result_metrics`), `:1923`, `:1941`, `:1956` — the order in which a
  request is validated, fingerprinted and recorded. This is what settles the
  fingerprint decision, and it is an ordering fact, not a workload fact.
- `src/core/error.rs:202` — the exhaustive `ErrorClass` match with no `other`
  arm. Ten classes are reachable, not the eleven the enum declares.
- `crates/telemetry/src/bridge.rs` and `src/core/trace_context.rs` — the W3C
  traceparent bridge that shipped in `1a28d92`, after this plan's §2 was
  written. §2 now has to say why it does not serve the manifest hop.
- `crates/telemetry/src/sampling.rs:66-74` — `is_always_sampled`, and the
  reason §3's candidate 2 was wrong about full-scan spans being kept. Now
  `:124-143` after the BUG-3 fix (`8f150b2`), which also deleted the
  `turbolay.query.full_scan` arm this function used to read.
- `src/core/cache.rs:237` — `resident_bytes` is already O(1), which strikes
  §5's proposed remedy for the cache gauges before it is written.
- `src/shard/lifecycle.rs:285-330` — the two gauge functions, read for their
  guard *scoping* rather than their lock count. That is where BUG-1 was.
- `src/engine/cluster.rs:1188`, `:1216-1230` — `cluster_for_scope`. BUG-2.
- `src/query/coordination.rs:59`, `:926`, `:1211` — the query transport's
  timeout, its slow-query threshold and its `remote_latency_us`. Two of the
  three histogram bucket bounds in §1.10 are these constants, not taste.
- `src/bin/graph_node/config.rs:223` — `DEFAULT_MAX_QUERY_RUNTIME_MS`, the
  third forced bound.
- `~/.cargo/registry/.../opentelemetry-0.32.0/src/metrics/instruments/mod.rs`
  and `~/.cargo/registry/.../opentelemetry_sdk-0.32.1/src/metrics/` — read a
  second time, this time for what is *absent*: there is no observable
  histogram and no `MetricProducer`. §1.5's correction turns on that absence.
- `~/.cargo/registry/.../opentelemetry_sdk-0.32.1/src/metrics/internal/mod.rs`
  — `ValueMap::measure`, which is why §1.10 does not use OTel's own
  `Histogram` type in the query path.

## What the code audit falsified

The prior plan has a section with this name, written after implementation. This
one is written *before* implementation, from a second read of the same files at
`6255ca3`. That is a weaker instrument and it still found nine things, three of
which are live bugs. They are recorded here rather than silently corrected in
the prose below, for the same reason the prior plan gives: each was believed on
the strength of a code read, and a second code read is the cheapest thing that
disproves one.

### Three live bugs

**BUG-1 — the cache gauges hold seven mutexes at once.**
`graph_cache_entry_counts` and `graph_cache_resident_bytes`
(`src/shard/lifecycle.rs:285`, `:317`) built their return value as a struct
literal in tail position, with each field initialised directly from
`lock().await`. The struct literal *is* the function's tail expression, so
every `MutexGuard` temporary lived until the function returned: seven mutexes
held simultaneously in the first function, five in the second, each acquisition
an await point, all of them read-path mutexes. `BoundedGraphCache::get` takes
`&mut self` for the LRU clock bump, so even a cache *hit* needs the exclusive
lock. This is not twelve cheap acquisitions once a minute — it is a convoy that
serialises the read path for the duration of the collection.

Fixed by binding each lock to a `let` before the struct literal, which turns
twelve overlapping acquisitions into twelve disjoint O(1) ones with zero
behaviour change. **Fixed in the working tree at the time of this amendment.**
§1.5 and §5 both reasoned about this code and both counted the locks correctly
while missing that they overlapped.

**BUG-2 — `cluster_for_scope` holds the `clusters` mutex across a shard open.**
`src/engine/cluster.rs:1216-1230` holds `clusters` across
`open_promotable_scoped_with_memory_options(...).await`. A shard open is
multi-millisecond, and every other taker of that mutex — including the metrics
collector §1.5 proposes, and the read path itself via `cluster_for_scope`
(`:1188`) — inherits the stall.

**Not fixed, and deliberately left open.** Narrowing the critical section is not
a mechanical change: releasing the lock around the open admits a double-open
race between two callers for the same scope, so the fix needs a
one-open-per-scope guard (an in-flight map, or a per-scope `OnceCell`) designed
on its own terms. It is recorded here so it is not lost, because it is the
reason M2's "measure the collector against read-path p99" step could produce a
confusing result: with BUG-2 present, a slow collection and a slow read can
share a cause that has nothing to do with the cache gauges.

**BUG-3 — `SAMPLING_FORCE` never fires.** `is_always_sampled`
(`crates/telemetry/src/sampling.rs:66-74`) reads `turbolay.sampling.force` and
`turbolay.query.full_scan` from the attributes present when the sampling
decision is taken. All seven sites that set them do so by `span.record` **after
entering the span**, having declared the field `tracing::field::Empty` at
creation: `src/query/opencypher.rs:353`, `src/client/service.rs:1923`,
`src/shard/query_optimizer.rs:963` and `:1047`, `src/shard/query.rs:540`,
`:5333`, `:5409`; `turbolay.query.full_scan` likewise (recorded at
`query_optimizer.rs:959`, `Empty` at `:1061`).

So the force flag has never once been honoured. Only the three
`ALWAYS_SAMPLE_SPANS` names fire, and only on roots. Every error trace is
ratio-sampled at 5%, which means the `corruption`, `kernel` and `config`
classes read zero for hours at a time. The existing unit tests pass because
they hand attributes directly to `should_sample` and never exercise the
`field::Empty` + `record` path; the regression test must reproduce the real
path or it will pass against the bug.

**Fixed in `8f150b2`, and driving the real path corrected this entry twice.**

*The decision is taken lazily at a span's **first entry**, not at creation.*
This paragraph originally said "at span start", which is imprecise in the one
direction that matters: `span.record` **before** first entry does reach the
sampler, because it still lands in the builder. `client_root_span`
(`src/client/service.rs:1979-2026`) already depends on that — `correlation_id`,
`caller.step` and `runtime_limit_ms` are declared `Empty` and recorded before
the span is ever entered, and they are visible to `should_sample`. So the rule
is an *entry-ordering* trap, not a flat prohibition on `record`, which is
precisely why the fix is a new key rather than a reordering: a reordering would
work and then silently stop working the next time a call site moved a line.

*Hoisting the attribute to creation time would have fixed **zero** of the seven
sites, not some of them.* At all seven the value does not exist until after the
work has run, and most sit on child spans (`query.plan`, `query.execute`,
`write.bookmark`, `artifact.lookup`, `storage.wal_tail`) where the sampler
defers to the parent entirely — so even a perfectly timed force would have been
a second no-op behind the first.

The keep-intent at those sites is therefore a **tail** decision, permanently,
and the fix splits one key into two:

| Key | Reachability |
|---|---|
| `turbolay.sampling.force` | head sampler; **creation-time and root-only**, both limits now documented at `semconv.rs:174-192` and `sampling.rs:33-60` |
| `turbolay.sampling.tail_keep` | new (`semconv.rs:194-214`); carries a reason — `SAMPLING_TAIL_KEEP_ERROR` = `error`, `SAMPLING_TAIL_KEEP_FULL_SCAN` = `full_scan` — so the collector can retain the two at different rates. Inert in this process |

All seven sites now record `tail_keep`; none records `force`.

`is_always_sampled` **no longer honours `turbolay.query.full_scan` at all**, and
that removal is deliberate rather than incidental. It was dead twice over, and
reviving it by hoisting the field would be worse than the dead code: full scans
are not rare in an analytics workload, so the configured ratio would silently
become 100% for a whole class of query. A sampler must not key off a *data*
attribute; `sampling.force` exists for no purpose but to say "keep this", which
is why it is the only thing read.

**What this hands to deployment.** `tail_keep` does nothing until a collector
runs a `tail_sampling` processor keyed on it; the YAML is in the sampling module
docs (`sampling.rs:62-89`). One consequence is documented rather than hidden: a
trace the *head* sampler dropped never reaches the collector, so the tail policy
can only rescue what the head kept. Run the head at ratio 1.0 if error coverage
matters more than export bandwidth.

This is the same class of mistake the prior plan's implementation record already
contains — "`span.record()` destroyed the fields set at span creation" — and it
is the second time the deferred-attribute pattern has broken something
downstream of span creation. The generalisation, stated precisely this time:
*anything that reads a span's attributes at head is reading the set present when
the span was first entered, not the set recorded during it.*

BUG-3 was load-bearing for two claims in this document. §1.7's consolation prize
("filter the traces on the same attribute") and §3's candidate 2 ("sampling is
handled, so this rate is exact") were both false while it stood. Both are
corrected in place below — and both remain contingent, because the code fix
moves the decision to the collector rather than making it here.

### The structural correction to §1.5

**OpenTelemetry has no observable (asynchronous) histogram, and the Rust SDK
has no `MetricProducer`.** `opentelemetry` 0.32 offers
`u64_observable_counter`, `f64_observable_up_down_counter` and
`u64_observable_gauge`, but for histograms only the *synchronous*
`f64_histogram` / `u64_histogram`; and `opentelemetry_sdk` 0.32.1 has no
`MetricProducer` trait to register an external source against. §1.5's central
device — a cached snapshot read synchronously behind observable instruments —
therefore covers counters and gauges and **does not extend to histograms at
all**. The consequence is written into §1.5 and designed around in §1.10.

### Smaller corrections, all of them factual

- **The registry has 31 `turbolay.*` keys, not 26.** §1.3 and §1.4 classify 26,
  so the partition test §1.4 proposes would fail on the day it was written.
  The seven unclassified keys are `PLACEMENT_STATE`, `PLACEMENT_PREVIOUS_STATE`,
  `PLACEMENT_LIVE_NODES`, `WRITER_REOPEN_DELAY_MS`, `WRITER_REOPEN_CAP_MS`,
  `CONSISTENCY` and `WRITER_RETRIES`. §1.3 now places them. **32 as of
  `8f150b2`**, which added `SAMPLING_TAIL_KEEP` to `ALL_TURBOLAY_KEYS`; §1.3
  places it span-only alongside `SAMPLING_FORCE`, so the partition holds.
- **`error.class` is not in `ALL_TURBOLAY_KEYS`.** It is not `turbolay.`-
  namespaced (`semconv.rs:172`), and neither is `db.system.name` (`:182`). The
  proposed test iterates `ALL_TURBOLAY_KEYS`, so it would silently never
  classify the one attribute §1.3 puts *first* on the safe-label list. An
  `ALL_REGISTRY_KEYS` superset is required; §1.4 now says so.
- **§1.6's "both read the same lock-free atomics" is false** for the two gauge
  sets. Neither export is lock-free there — that is the whole subject of BUG-1.
  The claim is true of the three counter structs and only of those.
- **`acquire_local_write_guard` has 28 call sites, not 29.** The original count
  included the definition. Cited twice, in "What we found first" and in §3
  candidate 1. It changes nothing about the argument; it is corrected because
  this document said its numbers would be checked.
- **`ErrorClass` has 10 reachable values, not 11.** See §1.3.
- **§2's premise is stale.** A W3C traceparent bridge shipped in `1a28d92`
  (`crates/telemetry/src/bridge.rs`, `src/core/trace_context.rs`) after §2 was
  written. §2 now has to explain why it does not serve the manifest hop rather
  than write as though no propagation existed.

### Drifted line references

The prose was written against `02eba8c`. These are the citations that moved.

| Cited | Correct at `6255ca3` |
|---|---|
| `src/engine/cluster.rs:1238` (Sources, §1.5) | **`:1265`** — `local_shard_runtime_metrics` |
| "scoped-cluster mutex at `:1239`" (§1.5) | **`:1266-1272`** |
| `log_fence_attribution` `src/core/state.rs:455` (§"What we found first", §3) | **`:493`** — and it was wrong when written; `:455` was the call site, now `:436` |
| `src/engine/index_store.rs:11` (Sources, §2) | **`:12`** — `:11` is the derive attribute |
| `semconv.rs:127-141` (§1.3) | **`:142-156`** |
| `semconv.rs:229-237` (§1.4) | **`:249-257`** |

`semconv.rs` shifted by a rule, which is worth stating so the next citation can
be repaired without a diff: line numbers ≤120 are unchanged, 121–208 shifted by
**+15**, and ≥209 by **+20**. No other file this document cites changed between
`02eba8c` and `6255ca3`.

### Confirmed, and not to be re-litigated

Four claims survived a second, independent read and should not be checked a
third time.

- `/metrics` exports **8 of 65** counters (35 operational + 19 cache + 11
  client). Exact. The whole fleet maintains 72 including the indexer's seven
  counters — the indexer's nine *values* are seven counters plus `ready`, a
  gauge, and `last_success_ms`, a timestamp — but the indexer is a separate
  binary and its seven are not part of the 65.
- The cardinality trap is sprung, and worse than stated: `scope` is a label on
  all five per-shard series (`admin.rs:161`, `:162`, `:163`, `:199`, `:226`),
  and `validate_component` (`src/codec.rs:175-187`) bounds only the *character
  set* of a scope component. There is no length limit, and `MAX_NAMESPACE_DEPTH
  = 8` bounds depth, not breadth. The label is unbounded in the strong sense.
- `turbolay.query.access_path` is unbounded (`src/shard/query_optimizer.rs:862`).
- `writer.fence_refresh` is on the write path (`src/shard/lifecycle.rs:262`,
  `:523`).

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

It is also unbounded in the strong sense, which the original draft only
implied. `validate_component` (`src/codec.rs:175-187`) constrains the
*character set* of a scope component and nothing else — no length limit — and
`MAX_NAMESPACE_DEPTH = 8` bounds how deep a namespace may nest, not how many
distinct ones may exist. Nothing in the write path bounds the number of scopes
a fleet will see.

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

`crates/telemetry/src/sampling.rs:38-43` (now `:97-102`) lists `writer.fence_refresh` among the
spans that are "low-volume and high-value" and always sampled. `refresh_writer_fence`
(`src/core/state.rs:395`) is called from exactly two places —
`acquire_local_write_guard` (`src/shard/lifecycle.rs:262`) and
`validate_write_fence` (`:523`) — and `acquire_local_write_guard` has 28 call
sites. It runs once per write, not once per incident.

The always-sample rule only fires for spans with no valid parent
(`sampling.rs:115-130`, now `:183-195`), so under a `client.mutate` root the parent decides and
nothing is over-sampled. But the *span* is high-volume, and it matters for §3:
the ping-pong alert cannot be "fence refreshes are happening". The
distinguishing fields — `turbolay.writer.last_promoted_by` and its two
companions — are recorded only in the fence arm (`src/core/state.rs:426-442`,
via `log_fence_attribution`, defined at `:493` and called at `:436`), so they
are present on a small subset
of a large population. Any alert must key on those, and any dashboard must
carry both numbers or the rate will look catastrophic.

## 1. OTel metrics

### 1.1 Recommendation

Add an OTel meter provider to `crates/telemetry`, fed by **observable
(asynchronous) instruments** whose callbacks read a snapshot cached by a
separate tokio task. Export **all 65 counters and both gauge sets**, from both
binaries. **Duplicate `/metrics` rather than replace it**, and treat that
duplication as permanent, not transitional. Enforce the label allowlist with a
type in `semconv.rs`, not a convention.

Two additions from the 2026-07-27 amendment. The naming scheme is **not** one
vocabulary: it is the OTel semantic-convention name where one genuinely exists
and `turbolay.*` everywhere else (§1.9). And three of the fifteen duration
counters become **real histograms in the kernel** (§1.10), which is a kernel
change this document originally put out of scope and now does not.

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

The registry (`crates/telemetry/src/semconv.rs`) currently classifies its
**31** `turbolay.*` keys informally, in a doc comment, into "bounded and safe
anywhere", "bounded in practice", and "spans only". That is not enough
resolution to build a meter on. The classification below is the one to encode.

(The original draft said 26 and classified 26. That was a miscount, and it
matters more than a miscount usually does, because §1.4's whole mechanism is a
test asserting the classification is *total*. Seven keys were missing; they are
placed at the end of this section.)

**Safe as metric labels.**

| Attribute | Bound | Evidence |
|---|---|---|
| `turbolay.placement.ownership` | 4 | closed vocabulary `local`/`remote`/`unowned`/`unknown`, `semconv.rs:105-119`, recorded at `src/engine/cluster.rs:528-551` |
| `turbolay.kernel` | 3 | the sparse-kernel ladder: `Adjacency`, `CompactCsc`, `SuiteSparse` |
| `turbolay.outcome` | 3 | `Outcome` — success / skipped / failed |
| `error.class` | 10 | `ErrorClass`, `crates/telemetry/src/error_class.rs:29-58` — but see below |
| `turbolay.cell_id` | cells per node | configured, `GRAPH_CELLS`, `src/bin/graph-indexer.rs:231` |
| `turbolay.edge_type` | schema | relationship types the tenant defines |

`error.class` is **10**, not the 11 the enum declares. `GraphError::class`
(`src/core/error.rs:202`) is an exhaustive match with no `other` arm, and
`ErrorClass::Other` is constructed nowhere in the tree. The eleventh value
exists in the type and cannot reach a label. This is the kind of number that
should be right, because it is the one an operator uses to decide whether a
`{cell_id, edge_type, error_class}` series is affordable.

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
| `turbolay.correlation_id` | one value per request, unbounded by construction; `semconv.rs:142-156` already says this in the strongest terms |
| `turbolay.caller.step` | caller-supplied and untrusted. The prior plan's own worked example is `delete_source.delete_relates_batch_3_0` — a *batch index* inside the label. Unbounded by construction, and supplied by a process Turbolay does not control |
| `turbolay.query.access_path`, `turbolay.query.optimizer_passes` | comma-joined sequences, combinatorial — see "What we found first" |
| `turbolay.query.fingerprint` | minted *before* validation, so an authenticated client mints series directly. Not workload-dependent — attacker-reachable. See §5 |
| `turbolay.read_epoch`, `turbolay.commit_epoch`, `turbolay.base_sequence`, `turbolay.writer.epoch`, `turbolay.writer.last_promoted_epoch`, `turbolay.writer.last_promoted_at` | monotonic. A new value on every write or every tick |
| `turbolay.generation` | a SHA-256 hex digest, `src/engine/index_store.rs:129`. A new value on every rebuild |
| `turbolay.query.rows_estimated`, `turbolay.query.rows_returned` | measurements. They are what a histogram *records*, not what it is keyed by |
| `turbolay.sampling.force`, `turbolay.sampling.tail_keep` | sampler controls, meaningless to a meter. Two keys since `8f150b2`: the first is the head sampler's, creation-time and root-only; the second is the collector's tail-sampling input and is inert in-process. See BUG-3 |

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

**The seven keys the original draft did not classify.**

| Attribute | Placement | Why |
|---|---|---|
| `turbolay.placement.state` | **label**, 3 | closed vocabulary, `crates/placement/src/liveness.rs:167-173`, pinned by the test at `:664-669` |
| `turbolay.placement.previous_state` | **label**, 3 | same vocabulary, same test |
| `turbolay.placement.live_nodes` | span-only | a count. A measurement, not a key |
| `turbolay.writer.reopen_delay_ms` | span-only | a duration. Measurement |
| `turbolay.writer.reopen_cap_ms` | span-only | a duration. Measurement |
| `turbolay.writer.retries` | span-only | a count. Measurement — and note the prior plan put `shard.write_txn` on the retry loop *specifically* to carry it |
| `turbolay.consistency` | span-only | a per-request mode. Small vocabulary, but nothing pins it and no metric asks the question |

The two placement states earn label status for the same reason
`placement.ownership` does: a closed vocabulary with a test standing on it.
`placement.state` is also what §3's candidate 5 alerts on, and candidate 5 is
explicitly a *count of distinct instances in `shed`* — a question a metric can
answer only if the state is a dimension.

The other five are all measurements. That is the same distinction the table
above draws for `rows_estimated` and `rows_returned`, and the fact that five
more keys land on the same side of it is a small argument that the distinction
is the right one: a registry key is either something you *group by* or
something you *record*, and the ones that are recorded are exactly the ones
§1.10 turns into buckets.

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
pub const L_PLACEMENT_STATE: MetricLabel = MetricLabel(PLACEMENT_STATE);
pub const L_PLACEMENT_PREVIOUS_STATE: MetricLabel = MetricLabel(PLACEMENT_PREVIOUS_STATE);
pub const L_QUERY_FULL_SCAN: MetricLabel = MetricLabel(QUERY_FULL_SCAN);
pub const L_DB_SYSTEM_NAME: MetricLabel = MetricLabel(DB_SYSTEM_NAME);
pub const L_LE: MetricLabel = MetricLabel(LE);   // §1.10, bucket upper bound

pub const METRIC_LABELS: &[MetricLabel] = &[/* the eleven above */];
pub const SPAN_ONLY_KEYS: &[&str] = &[/* the twenty-three others */];
```

The constructor stays private to the module, so `MetricLabel` cannot be built
from an arbitrary string outside `semconv.rs`. Every meter helper takes
`&[(MetricLabel, &str)]` rather than `&[KeyValue]`. Passing `SCOPE` to a metric
is then not a policy violation to be caught in review — it is a type error.

Three of those eleven were not in the original list and are worth a line each.
`placement.state` and `placement.previous_state` are §1.3's late additions.
`db.system.name` is a *consequence of §1.9's naming decision*: it has exactly
one value (`neo4j`, `semconv.rs:185`), it costs nothing, and it is the
attribute the vendor's database view keys off — putting `db.*` names on the
wire and then omitting it would spend the cost of the decision and collect none
of the benefit. `le` is §1.10's bucket bound; it needs a registry constant
rather than a bare string precisely so the partition test below stays total.

**The partition test as originally written would not have worked.** It is still
the right idea, and it is still the direct descendant of
`no_registry_key_is_redacted` (`semconv.rs:249-257`), but it iterates
`ALL_TURBOLAY_KEYS` — and `error.class` (`semconv.rs:172`) and `db.system.name`
(`:182`) are not `turbolay.`-namespaced and so are not in that list. The test
would have passed while never classifying the attribute §1.3 puts first on the
safe-label list. It needs a superset:

```rust
/// Every key this crate defines, `turbolay.`-namespaced or not.
/// `ALL_TURBOLAY_KEYS` remains the namespaced subset the redaction tests use.
pub const ALL_REGISTRY_KEYS: &[&str] =
    &[/* ALL_TURBOLAY_KEYS, then ERROR_CLASS, DB_SYSTEM_NAME, LE */];

#[test]
fn every_registry_key_is_classified_exactly_once() {
    for key in ALL_REGISTRY_KEYS {
        let label = METRIC_LABELS.iter().any(|l| l.key() == *key);
        let span_only = SPAN_ONLY_KEYS.contains(key);
        assert!(label ^ span_only, "{key} is in neither list, or in both");
    }
}

#[test]
fn every_metric_label_is_a_registry_key() {
    for label in METRIC_LABELS {
        let key = label.key();
        assert!(ALL_REGISTRY_KEYS.contains(&key), "{key} is not in the registry");
    }
}
```

The second test is the one that catches the `le` mistake — a `MetricLabel`
built from a string that was never added to the registry would otherwise
silently escape the partition, which is exactly the hole `error.class` was
already sitting in. With both tests, 34 keys partition into 11 labels and 23
span-only, and neither list can gain a member without the other being
considered.

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
(`src/engine/cluster.rs:1265`), which is `async` — it locks the scoped-cluster
mutex at `:1266-1272`, and per shard calls `graph_cache_entry_counts`
(`src/shard/lifecycle.rs:285`, seven `Mutex::lock().await`) and
`graph_cache_resident_bytes` (`:317`, five more). Twelve async cache-mutex
acquisitions per cell per collection, on the same mutexes the read path takes
on every cache lookup. You cannot `.await` any of that inside an OTel callback,
and `block_on` inside a callback that the SDK runs on its own OS thread
(`periodic_reader.rs:171`) is how you deadlock a node.

Two things about that path were wrong when this was written, and both are
worse than the version above rather than better. Those twelve acquisitions were
*overlapping*, not sequential — BUG-1, now fixed, and the reason "twelve cheap
locks once a minute" was not the right way to think about the cost. And the
scoped-cluster mutex at `:1266` is itself held across a shard open elsewhere in
the same file — BUG-2, not fixed. The interval task will therefore occasionally
block for a shard open before it takes a single cache lock, which is worth
knowing before anyone attributes a collection stall to the gauges.

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

That argument is now made, in §1.10, for three of the fifteen. Everything above
stands: the twelve that stay are still sums, still exported as
`ObservableCounter`, and `rate/rate` is still the only honest thing to do with
them.

**Correction: this section's device does not extend to histograms.** The whole
shape above — a cached snapshot read synchronously behind an observable
instrument — depends on the instrument having an *observable* form.
`opentelemetry` 0.32 provides `u64_observable_counter`,
`f64_observable_up_down_counter` and `u64_observable_gauge`; for histograms it
provides only the synchronous `f64_histogram` and `u64_histogram`. There is no
observable histogram in the API, and no `MetricProducer` trait in
`opentelemetry_sdk` 0.32.1 to register an external source against either. Both
escape hatches are absent, not merely awkward.

The consequence is concrete and belongs in the runbook alongside the `scope`
divergence: **bucket counts reach OTLP as a family of `ObservableCounter`s
carrying an `le` label**, one series per bucket, rather than as an OTLP
histogram data point. Semantically that is the same information — it is what a
Prometheus histogram *is* — and `histogram_quantile` over the family works. But
a vendor's native latency widget looks for a histogram data point and will not
light up on a family of sums. That is a real cost of the SDK's shape and it is
paid whichever naming scheme §1.9 picks; do not let anyone conclude the
instrument was chosen carelessly.

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

*Duplication is nearly free here — for the counters.* The marginal cost of the
second export is one more read of a `Vec` the interval task already built. This
is not the usual "two systems, two truths" trap, because there is exactly one
source of truth — the `AtomicU64`s — and both exports are pure functions of it.

The original draft said "both read the same lock-free atomics", and **that is
false for the two gauge sets.** `GraphCacheEntryCounts` and
`GraphCacheResidentBytes` are not atomics and neither export path is lock-free
there: both go through the twelve cache-mutex acquisitions of
`src/shard/lifecycle.rs:285` and `:317`. The claim is true of
`GraphOperationalMetricsSnapshot`, `GraphCacheMetricsSnapshot` and
`ClientQueryMetricsSnapshot`, and true of nothing else.

It matters because "free" was the argument for duplication, and for the gauges
duplication is not free: `/metrics` pays the locks once per scrape and the
interval task pays them again once per export interval. Aligning the two
intervals is not enough — they are unsynchronised. Either accept two collections
a minute (the recommendation: it is twelve O(1) acquisitions each, post-BUG-1),
or have `/metrics` serve the interval task's cached `Arc` instead of collecting
its own, which is a change to `/metrics` and therefore out of scope by §6. The
first option is right; it is written down so the second is not rediscovered as
though it were free.

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
module docs (`:1-31`, now `:1-89`) explain that keeping error traces is a *tail* decision
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

**With one caveat that BUG-3 introduces, and that the fix moved rather than
removed.** That consolation prize assumes the full-scan spans are actually in
the backend to be filtered. They were not: the sampler never saw
`turbolay.query.full_scan`, because it is recorded after the span is entered
and `is_always_sampled` is handed the attributes present when the span is
**first entered** (`sampling.rs:66-74`, now `:124-143`). A full-scanning query's spans were
ratio-sampled at 5% like everything else, so "filter the traces on the same
attribute" returned one in twenty.

`8f150b2` did **not** make that rate exact, and deliberately so. Reviving the
`full_scan` arm was rejected outright — a sampler keying off a data attribute
would silently turn the configured ratio into 100% for a class of query that is
common in an analytics workload — so `is_always_sampled` no longer reads
`turbolay.query.full_scan` at all. Instead the planner records
`turbolay.sampling.tail_keep = full_scan`, which is the collector's input. The
navigation story is therefore contingent on **deployment** running the
`tail_sampling` policy (`sampling.rs:62-89`), and on the head ratio being high
enough that the traces the tail policy wants still reach it. At ratio 0.05 the
tail policy can only rescue the one in twenty the head already kept.

### 1.8 Sequencing

**Two steps come before M1, and neither is a metrics step.**

**BUG-3, the sampler fix, first. Done — `8f150b2`.** It was the smallest change
here and the most load-bearing: §1.7's navigation story, §3's candidate 2 and
every "read it off the traces" fallback in this document were false while it
stood. Fixing it also changes what the staging week will show, so doing it after
the baseline would have meant taking the baseline twice.

**But it discharged the code half only.** The fix routes post-hoc keep-intent to
`turbolay.sampling.tail_keep`, which nothing in this process acts on. Until the
collector runs the `tail_sampling` policy (`sampling.rs:62-89`), error and
full-scan traces are still ratio-sampled exactly as before — so the baseline week
must not start until that policy is deployed, or it will be a baseline of the
old behaviour under a new attribute name.

**BUG-1, the guard scoping, second.** M2 is explicitly the step that measures
collection cost against read-path latency; measuring it with the convoy present
would measure the convoy.

**Then H1, before M1** — see §1.10. The ordering is not aesthetic. M1 writes
the `ObservableCounter` field list and the one-enumeration test that ties the
meter's instrument list to `append_node_metrics`. H1 changes which fields exist
on three of those snapshots — `execution_duration_us` and two others stop being
standalone counters. Doing M1 first means writing that field list and that test
against a shape H1 immediately amends, so both get written twice and the second
writing is a merge rather than a decision.

The full order is: **BUG-3 → BUG-1 → H1 → M1 → H2 → M2 → M3 → H3.** BUG-3 landed
in `8f150b2` and BUG-1 was fixed in the working tree at the amendment; H1 is next,
with the collector `tail_sampling` policy as a deployment task running alongside.

**Step M1 — the meter provider and the operational counters.** Add
`"metrics"` to `opentelemetry-otlp` in the root `Cargo.toml`; add
`SdkMeterProvider` to `otlp::Providers` (`crates/telemetry/src/otlp.rs:43`) and
to its `shutdown` (`:50`); add `MetricLabel`, `METRIC_LABELS`, `SPAN_ONLY_KEYS`,
`ALL_REGISTRY_KEYS` and both partition tests to `semconv.rs`; add the interval
task and the `ObservableCounter` set for `GraphOperationalMetricsSnapshot` and
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

**After H3, and outside the metrics work: the `#[instrument]` migration.**
`#[instrument]` is the standard for new spans from here, and the existing
hand-rolled `span!` + `enter` sites migrate to it. One rule is non-negotiable
and belongs next to the decision rather than in a review comment: **always
`skip_all` (or an explicit `skip(...)`) plus explicit `fields(...)`.** The macro
records every function argument by default, which on the query path means query
text and bind parameters would be recorded as span fields — defeating
`crates/telemetry/src/redact.rs`, which is a denylist and cannot know the names
the macro invents. The migration is sequenced last because it touches many
files and changes no behaviour, so it is the step most safely interrupted.

### 1.9 Naming: `db.*` where a semantic convention genuinely exists

**Decision: use the OTel semantic-convention name wherever one genuinely
exists, and `turbolay.*` for everything else.** §5 left this open and leaned
the other way — one vocabulary, `turbolay.*` throughout, on the argument that a
dashboard and a trace view should feel like one system. The decision goes the
other way, for native database-viewer compatibility: the vendor's out-of-the-box
database view keys off `db.*`, and a metric named `turbolay.query.duration`
will never appear in it no matter how good it is.

**The evidence, including the part that argues against the decision.** Of the
72 counters across both binaries, **two** have a semconv name:

| Kernel quantity | Semconv name | Status |
|---|---|---|
| client query duration (§1.10's histogram 1) | `db.client.operation.duration` | **stable** |
| rows returned to the client | `db.client.response.returned_rows` | development |

The other 70 become `turbolay.*`. So the decision buys native rendering for one
stable metric and one unstable one, at the cost of a vocabulary split — an
operator reading a dashboard sees two namespaces and has to know which is which.
That is a real cost and it is the honest reason §5 leaned the other way. It is
outweighed because the one metric that does map is the *latency* metric, which
is the one every database view is built around and the one an operator reaches
for first.

**And the decision is deliberately non-conformant in one dimension.** Semconv
marks `db.namespace` as required-if-applicable on `db.client.*`. It is
applicable here — the namespace is the `scope` — and it is **omitted anyway**,
because `scope` is the unbounded tenant root §1.4 exists to keep off metrics.
Emitting a conformant `db.client.operation.duration` would mean emitting the
one label this entire section is written to prevent. So the series is emitted
without it, knowingly, and **this must be in the runbook** next to the `scope`
divergence, because a conformance checker will flag it and the flag will look
like a bug.

Two consequences to design around rather than discover:

- **The unit.** Semconv fixes `db.client.operation.duration` in **seconds**;
  §1.10's ladder is in microseconds because that is what the kernel measures
  in. The kernel keeps µs; the export layer divides the bucket bounds by 1e6
  for that one instrument. Since §1.10 exports the bounds once from the library
  (so the two renderings cannot disagree), this is one conversion in one place
  — but it has to be *there*, and a bound table in seconds must never leak back
  into the kernel.
- **The widget still will not light up.** §1.5's correction means the buckets
  reach OTLP as an `ObservableCounter` family with an `le` label rather than as
  a histogram data point. A vendor view that wants a histogram will not find
  one, whatever the metric is called. So the compatibility this decision is
  made for is *partial*: `db.system.name` and the metric name get the series
  into the database view's world; the native latency panel needs a data-point
  type the SDK cannot produce from a cached snapshot. Recording this here so
  nobody concludes, on seeing an empty panel, that the naming decision was
  implemented wrong.

`db.system.name` becomes a metric label as a direct consequence — one value,
`neo4j` (`semconv.rs:185`), and the attribute the database view keys on. See
§1.4.

### 1.10 Percentiles: a duration histogram in the kernel (H1–H3)

§1.5 says that if percentiles are wanted, "that is a change to the kernel's
counters, not to the export — and it should be argued on its own". This is that
argument. It is in §1 rather than in its own top-level section because it is the
same export, the same interval task and the same label discipline; what changes
is what the kernel records.

**Why now rather than later.** A mean is the wrong statistic for latency and
everyone knows it, but the operational trigger is narrower than that: the
`slow_queries` counter (`src/query/coordination.rs:4349`) fires at a 500 ms
threshold (`:926`) and there is currently nothing that reconciles with it. When
`slow_queries` rises, the only available follow-up is "the mean also rose,
somewhat". A histogram whose ladder includes 500 ms turns that into an
arithmetic identity: the mass above the 500 ms bound *is* the slow-query count,
and the two disagreeing is a bug in one of them.

#### The type, and why not any of the alternatives

**Not OTel's own `Histogram`.** `ValueMap::measure`
(`opentelemetry_sdk-0.32.1/src/metrics/internal/mod.rs`) takes an `RwLock` read,
hashes a `Vec<KeyValue>` for the map lookup, and then takes a
`std::sync::Mutex` *per attribute set*. A lock, a hash and a `Vec` allocation
per observation, against the single relaxed `fetch_add` this codebase currently
pays. It would be the first non-lock-free metric in the tree, and it would be
in the query path. There is also a structural reason: `crates/telemetry`'s
`Cargo.toml` forbids the kernel from depending on it, and the kernel is where
the observation happens.

**Not `hdrhistogram`** — roughly 1,500 buckets, which is superb for a local
profile and unexportable as series. **Not a DDSketch crate** — the bucket set is
not fixed, and neither Prometheus exposition nor OTel explicit-bucket histograms
can carry a bucket set that changes shape between scrapes.

**A fixed-bound array in `src/core/metrics.rs`, no new dependencies.** Seventeen
finite bounds in microseconds, giving an 18-element array whose last element is
the `+Inf` overflow, plus a `sum_us`:

```rust
[100, 250, 500, 1_000, 2_500, 5_000, 10_000, 25_000, 50_000,
 100_000, 250_000, 500_000, 1_000_000, 2_500_000, 5_000_000, 10_000_000, 30_000_000]
```

Three of those bounds are **forced by code, not chosen by taste**, and that is
the reason to write them down rather than reach for a round-number ladder:

- **500 ms** is `slow_query_log_threshold` (`src/query/coordination.rs:926`).
  Above, and the tail cannot be reconciled with the `slow_queries` counter.
- **30 s** is both `DEFAULT_MAX_QUERY_RUNTIME_MS`
  (`src/bin/graph_node/config.rs:223`) and `DEFAULT_QUERY_TRANSPORT_TIMEOUT_MS`
  (`src/query/coordination.rs:59`). It is where the system gives up, so it is
  where the last finite bucket must end; everything past it is a timeout, not a
  slow query.
- **100 µs** is the floor because nothing user-facing completes below it except
  a pure cache hit. Buckets below it would all be the same bucket.

**No `count` field.** Derive it by summing the buckets, so `_count` and
`le="+Inf"` agree by construction rather than by two `fetch_add`s staying in
step. **No min/max** — each needs a CAS loop, and neither has anywhere to go in
Prometheus or OTLP exposition.

**`#[repr(align(64))]` is load-bearing.** Without it the bucket array shares a
cache line with whatever `AtomicU64` the struct layout puts next to it, and the
histogram's writes invalidate a neighbouring counter's line on every
observation. 152 B becomes 192 B padded, which is the cheapest 40 bytes in this
document. Cost per observation is roughly 10 ns — against the ~40 ns the
existing `Instant::now()` pair already costs at the same sites, so the marginal
cost of recording a distribution instead of a sum is under a third of what
measuring the duration already costs.

#### Convert three, not fifteen

There are fifteen `*_duration_us` counters. Three become histograms.

1. **`ClientQueryMetricsSnapshot::execution_duration_us`**
   (`src/client/service.rs:651`, recorded at `:1271`). Process-global, and both
   transports funnel through `execute_prepared_page_inner` (`:1163`), so one
   histogram covers Bolt and HTTP. **Split read from write on
   `QueryTransportAction`**: `record_result_metrics` (`:1900`) ignores the
   action today, so a mutation commit and a cached read land in the same
   distribution. That conflation is invisible in a mean and glaring in a p99,
   which is a small argument that the mean was hiding it rather than that
   nobody noticed.
2. **`GraphOperationalMetricsSnapshot::query_rows_duration_us`**
   (`src/core/metrics.rs:116`; sites `src/shard/query.rs:517`, `:4732`,
   `:4746`). Per-shard. **Label by `cell_id` only, never `cell_id × edge_type`**
   — an 18-series bucket family times 96 is 1,728 series per instrument per
   node, which is the point at which §1.3's arithmetic stops being affordable.
3. **`QueryTransportMetricsSnapshot::remote_latency_us`**
   (`src/query/coordination.rs:1211`). **Split client from server**: the same
   field is fed from an RPC round-trip (`:2331`, `:2346`) and from server-side
   executor time (`:4345`). `remote_latency` measured on the server is not
   remote, and mixing a network round-trip into the same distribution as local
   execution makes both unreadable.

**Not converting, and the reason is the same for all of them.**
`prepare_duration_us` has a narrow distribution and a mean describes it.
`graph_compute_queue_us` / `graph_compute_duration_us` are the strongest
"next", ranked fourth, and should wait for a question that needs them. The
artifact-lookup and graphblas-cache durations have one code path each. And all
the background durations — `gc_duration_us`, `verifier_duration_us`,
`artifact_build_duration_us`, `artifact_publish_duration_us` and the five
`bulk_import_*_us` — stay sums for three reasons that compound: they run of the
order of once a minute, so a p99 over five samples is noise; the
`artifact.build`, `artifact.publish` and `artifact.gc` spans already cover them
at effectively 100% sampling, so the distribution is already available where it
is actually asked for; and they would need a second ladder in
seconds-to-minutes, which is a second constant to keep in sync with a second
set of thresholds.

#### Backward compatibility, and one deletion that is not optional

**Delete the standalone `execution_duration_us: AtomicU64` and derive the
snapshot field from the histogram's sum** in `ClientQueryMetrics::snapshot()`.
Verified: it is written at exactly one site (`:1271`) and read at exactly one
(`:682`). Keeping both means two `fetch_add`s on one quantity and two numbers
that can disagree, which is the failure this document keeps arguing against
elsewhere. Same for `query_rows_duration_us` and `remote_latency_us`. The
public snapshot field keeps its name and type; only its provenance changes.

`DurationHistogramSnapshot` must derive `Clone + Debug + Default + Eq +
PartialEq`. It lands inside `ClientQueryMetricsSnapshot` and
`GraphOperationalMetricsSnapshot`, and transitively inside
`GraphShardRuntimeMetrics` (`src/engine.rs:119`) and
`ScopedGraphShardRuntimeMetrics` (`:128`), all of which derive all five.
`[u64; 18]` satisfies all five. **Do not use `Vec<u64>`** — it allocates on
every snapshot, and the snapshot is taken per shard per interval. **Do not add
`#[non_exhaustive]`** — it forbids `..Default::default()` for embedders, and
these structs are constructed that way in tests.

#### One enumeration, two exports

The library exposes `counter_fields()` and `histogram_fields()` keyed by the
**Rust identifier**. Neither exposition vocabulary — Prometheus `graph_*` names,
OTel `db.*`/`turbolay.*` names — appears in the kernel; the two name tables live
in the binary, which is where §1.6's "the two must not disagree about names"
test belongs. Add `every_histogram_field_reaches_both_exports` alongside it: it
fails `cargo test` until both names have been chosen for a newly added field,
which is the property §1.6 wanted and now gets for histograms too. The bucket
bounds are exported once from the library so the Prometheus rendering and the
OTLP rendering cannot disagree about where a bucket ends.

#### Steps

**Step H1 — the type and the three conversions. Kernel only, nothing
exported.** Add `DurationHistogram` / `DurationHistogramSnapshot` to
`src/core/metrics.rs`; convert the three sites; split read/write on
`QueryTransportAction` and client/server on `remote_latency_us`; delete the
three standalone `AtomicU64`s and derive their snapshot fields; add
`counter_fields()` / `histogram_fields()`.

**Done when** `/metrics` output is byte-identical to before — which it will be
**by construction**, not by luck: no duration counter is exported today
(`admin.rs:112-130` exports five client counters, none of them durations;
`:159-174` exports three operational, likewise), so H1 cannot change a byte of
it. And when the three derived snapshot fields equal what the deleted atomics
would have held, asserted in a unit test rather than eyeballed.

**Step H2 — export the bucket family.** One `ObservableCounter` per bucket per
histogram, carrying `L_LE`; plus `_sum`; `_count` derived from the buckets. The
`db.client.operation.duration` family renders its bounds in seconds per §1.9;
everything else stays in microseconds under a `turbolay.*` name. Both exports
fed from `histogram_fields()`.

**Done when** `histogram_quantile(0.99, …)` over the exported family in staging
agrees with a p99 computed directly from the same node's raw snapshot to within
one bucket, and the mass above the 500 ms bound matches the `slow_queries`
counter over the same window.

**Step H3 — the runbook and the dashboards.** Deliberately last, and a real
step rather than a documentation afterthought, because every failure mode of a
bucket histogram is a *misuse* failure and the misuses are predictable.

**Done when** the runbook states, in these terms:

- **The quantile is an estimate.** Worst-case relative error is about
  (bucket ratio − 1), which for this ladder is 20–30% in practice. It answers
  "is p99 10 ms or 100 ms" and "did it move 2×". It does **not** answer "did
  p99 go from 42 ms to 47 ms", and an alert phrased that way will be noise.
- **Do not average p99s across nodes.** Summing the bucket families and then
  applying `histogram_quantile` is correct; averaging per-node p99s is
  arithmetically meaningless. Nothing in the pipeline prevents the second, so
  it is dashboard discipline or nothing.
- **There is no per-tenant or per-fingerprint breakdown, and there will not
  be.** This is the first thing an operator will ask for, and the answer is no
  at this cost — see §5 on the fingerprint. "Which tenant got slow" remains a
  trace question.
- **Sum and buckets can skew by in-flight concurrency.** They are two
  independent relaxed `fetch_add`s, so a snapshot taken between them
  undercounts the sum by at most the number of observations in flight. `count`
  is derived from the buckets, so `_count` and `le="+Inf"` never skew.

**Revisit trigger, falsifiable.** The first time an alert of the form "p99 rose
more than X% week-over-week" fires on bucket-boundary noise, or fails to fire on
a real regression that stayed inside one bucket. Secondary: if any histogram
shows 90% or more of its mass in two adjacent buckets, **re-cut the ladder
first** — it is one constant — before anyone reaches for a sketch. Stating it
this way is the same discipline §2 applies to span links: a named condition
under which the decision is wrong, rather than "revisit if it proves
insufficient".

## 2. Span links from the write path to the indexing path

The prior plan adopted attribute-based correlation — `cell_id` plus
`generation` / `base_sequence` / `read_epoch` on all three paths — and deferred
span links because stamping a trace id into artifact metadata changes a
persisted storage format. That deferral was correct. Having now read the
format, it should be **deferred indefinitely**, and the reason is stronger than
"it changes a format".

### First, a premise that changed under this section

This section was written as though no trace context crossed a process boundary
in Turbolay. That is no longer true. `1a28d92` shipped a W3C `traceparent`
bridge — `crates/telemetry/src/bridge.rs` and `src/core/trace_context.rs` — so
a trace started on one graph-node continues on another across the query
transport, and the prior plan's Step 5b record explains what it cost.

It does not, however, do the job this section is about, and the reason is
worth stating precisely because "we already have propagation" is exactly the
argument someone will make against §2's conclusion.

The bridge propagates across a **synchronous RPC**: caller and callee overlap
in time, the caller's span is still open, and the context rides in a request
frame that is serde JSON with no `deny_unknown_fields` — invisible to an old
peer in both directions, no version bump, no rollout step. The write-to-index
hop is **store-and-forward**: the write commits and its trace ends; minutes or
hours later a *different process* reads a manifest out of object storage and
builds an index from it. There is no live request to carry a header on. The
only medium that spans the gap is the persisted manifest itself, and a manifest
is a strict-arity tab-separated line read by every node in the fleet — which is
the entire subject of the rest of this section.

So the two hops look alike and are not: one adds an optional field to a
self-describing wire format between two processes that are talking, and the
other adds a field to a storage format read by processes that have not been
deployed yet. The cheapness of the first is not evidence about the second.

### What the format change would actually be

There are two persisted manifests, both tab-separated single lines with a magic
first field and a strict field count.

`GraphIndexGeneration` — `src/engine/index_store.rs:12`, encoded at `:350`:

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
(`src/core/state.rs:493`, called at `:436`) reads the advisory cell-writer
record and the three fields are promoted onto the enclosing span.

**The trap, and it is specific.** The three `last_promoted_*` attributes are
recorded **only in the fence arm** (`src/core/state.rs:426-442`). Every other
`writer.fence_refresh` span — the overwhelming majority, one per write across
28 `acquire_local_write_guard` call sites — has them empty. An alert written
against "fence refresh spans" instead of "fence refresh spans with
`last_promoted_by` set" is measuring write throughput.

**Second trap: sampling.** With no client parent the span is always sampled by
name (`ALWAYS_SAMPLE_SPANS`, `sampling.rs:43`, now `:102`); under a
`client.mutate` root the parent decides at 5% (`:115-130`, now `:183-195`). A
distinct-count over a 5% sample systematically *under*-reports distinct values.
Either force full sampling for spans carrying `last_promoted_by`, or accept that
the alert detects sustained ping-pong and not a single exchange — and write which
one it is into the alert description.

BUG-3 narrows that choice, and `8f150b2` settles it. To be exact about the
starting point: **nothing on the fence path sets a sampling attribute at all.**
The fence arm (`src/core/state.rs:426-442`) records `turbolay.writer.epoch` and
`error.class` and nothing else; there is no `turbolay.sampling.force` site there,
none in `src/shard/lifecycle.rs`, and after `8f150b2` no `tail_keep` site either.
An earlier draft of this paragraph read as though such a site existed and was
ineffective. It does not exist.

What BUG-3 rules out is *adding* one. `turbolay.sampling.force` is now documented
as creation-time-and-root-only (`semconv.rs:174-192`), and `last_promoted_by` is
known only after the fence arm is taken — by which point the span has been
entered and the decision made. Hoisting the force to creation time is worse, not
better: the fence arm is not known there, so it would force *every*
`writer.fence_refresh` span, which is one per write. And under a `client.mutate`
root the span is a child, where the sampler defers to the parent and the force
attribute is not consulted at all.

So the remedy is a collector-side `tail_sampling` policy keyed on the attribute —
a deployment change, not a code change. If a code change is wanted alongside it,
it is one line recording `turbolay.sampling.tail_keep` in the fence arm, which is
the key the collector policy already reads; it is not in scope here and is not
what makes the alert work.

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

**Sampling is not handled in-process, and this was the plan's worst error.** The
original text read: "`RowQueryPlanSummary::record` sets
`turbolay.sampling.force` when `full_scan` is true
(`src/shard/query_optimizer.rs:960-963`), and the head sampler honours it
(`sampling.rs:72-74`). Full-scan spans are kept at 100%, so this rate is exact
rather than scaled." Every sentence of that is true in isolation and the
conclusion is false. The sampler did contain the code; it never ran on these
spans. `is_always_sampled` (`sampling.rs:66-74`) is handed the attributes present
when the span is **first entered**, and both `sampling.force` and
`query.full_scan` are declared `tracing::field::Empty` at creation and filled by
`span.record` after entry — at all seven sites. See BUG-3.

**What `8f150b2` changed, and what it did not.** `RowQueryPlanSummary::record`
now writes `turbolay.sampling.tail_keep = full_scan`
(`src/shard/query_optimizer.rs:960-966`) instead of `sampling.force`, and
`is_always_sampled` no longer reads `turbolay.query.full_scan` at all. That
second half is the load-bearing one for this candidate: the fix deliberately
declined to make full-scan spans force a keep. A sampler that keys off a data
attribute couples retention volume to the workload, and full scans are not rare
in an analytics workload, so the configured ratio would quietly become 100% for a
whole class of query.

So full-scan spans are *still* ratio-sampled at 5%, and the rate this candidate
is built on is *still* scaled by an unknown factor, until the collector-side
`tail_sampling` policy is deployed (`sampling.rs:62-89`). Three consequences:

- Candidate 2 cannot ship on the code fix alone. Not "would be better after" —
  the alert would be counting one full scan in twenty and calling it the rate.
  What it now waits on is a deployment change, not a code change.
- Even with the policy deployed, the tail sampler can only rescue traces the
  head kept. At head ratio 0.05 the rate is still scaled; it becomes exact only
  at a head ratio near 1.0.
- The claim that candidates 1 and 2 need *different* correction factors remains
  wrong in the same ironic direction: they need the *same* one, because neither
  is force-sampled and neither ever will be. It becomes true only once the two
  are separated by *collector* policy — `tail_keep` carries a reason (`error` vs
  `full_scan`) precisely so they can be, at which point the warning about mixing
  them on one dashboard applies exactly as written.

This is worth dwelling on because of how it was missed. The code was read, the
sampler was read, and the two were read separately. Nothing short of following
the attribute from `record` to `should_sample` would have caught it — which is
also why the existing unit tests passed: they hand attributes directly to
`should_sample` and never construct a span. The regression tests added by
`8f150b2` go through a real subscriber and assert on what a `SpanProcessor`
receives (`crates/telemetry/tests/head_sampling.rs`), and one of them drives the
real `RowQueryPlanSummary::record` in production's create-empty/enter/record
order (`src/shard/query_optimizer.rs:1181`). Both were verified failing against
the unfixed code.

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

### Candidate 5 — correlated placement shedding

**Signal.** The `placement view state changed` log carries
`turbolay.placement.previous_state`, `turbolay.placement.state`,
`turbolay.placement.live_nodes`, `node_id` and
`since_last_success_ms`. It is emitted only on a state transition, including
the recovery to `fresh`; the per-refresh LIST warning remains the detailed
store-error stream. `bolt.route` spans carry the same current state plus
`turbolay.placement.ownership`, so a routing refusal can be joined to the state
that caused it without parsing its error text.

**Alert on correlation, not one node.** One node entering `shed` can be a local
object-store or network fault and is exactly the case withdrawal is designed to
contain. Several `service.instance.id` values entering `shed` within one
`heartbeat_timeout` means the shared LIST dependency is failing and Kubernetes
is draining Service endpoints together. Page when the distinct-instance count
reaches the deployment's replica count; chart and ticket a single-node event.
Resolve on the matching transitions back to `fresh`.

**Sampling is not a problem.** The transition is an OTel log record, not a
sampled span, so the distinct-node count is exact. The `bolt.route` spans are
diagnostic context and may remain ratio-sampled.

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

## 5. Open decisions — all four resolved

All four were written with a **Settled by:** clause naming a staging
measurement. None of them needed one. Each was settled by reading a file, and
in three of the four the *premise* turned out to be wrong — the question was
asking about the wrong thing, so the measurement would have produced an answer
to a question nobody had.

That pattern is the most useful thing in this section, so each decision below
records three things: what was decided, what settled it, and where the original
framing was wrong. The third is the part worth reading.

### 5.1 The cache gauges — **keep, at 60s, conditional on the BUG-1 fix**

**Decided.** Keep both gauge sets on the 60s interval. Do not give them a
separate slower interval. Do not make `resident_bytes` incremental.

**What settled it.** Two reads. `src/core/cache.rs:237` — `resident_bytes` is
**already O(1)**; it is a maintained field, not a walk of the cache. And
`src/shard/lifecycle.rs:285`, `:317` — twelve `Mutex::lock().await` on an
uncontended mutex is roughly 200–400 ns per cell, once a minute. Neither number
needs a staging week; both are visible in the source.

**Where the premise was wrong.** The question was "are twelve lock acquisitions
too expensive". The acquisitions were never the risk. The risk was that all
seven of them were held **simultaneously** — BUG-1 — because the struct literal
they initialised was the function's tail expression, so no guard dropped until
the function returned. That is a convoy on read-path mutexes, not a cost per
acquisition, and it would not have shown up as "the gauges are a bit expensive"
in a p99 comparison. It would have shown up as a read-path latency cliff under
concurrency, at which point the p99 experiment would have been blamed on the
wrong thing.

And it is a **code fix, not a measurement**. Twelve overlapping acquisitions
became twelve disjoint ones by binding each to a `let`, with zero behaviour
change. The proposed experiment — 60s versus 10s, M2 on versus off — would have
measured the convoy and concluded the gauges were expensive.

The second half of the original remedy is struck outright: "or `resident_bytes`
needs to be maintained incrementally rather than computed on read" describes
work that was already done before the sentence was written.

**What survives.** M2's done-when — p99 of `query.execute` unchanged with the
collector on — is still worth checking, but now as a regression check rather
than as a decision procedure. And it should be run after BUG-2 is understood,
since a stall in `cluster_for_scope` will look exactly like a slow collection.

### 5.2 `turbolay.query.fingerprint` as a metric label — **no, unconditionally, forever**

**Decided.** Span-only. No opt-in flag, no hard cap, no overflow bucket. The
`METRIC_LABELS` list must never contain it.

**What settled it.** An ordering, not a distribution. The fingerprint is minted
at `src/client/service.rs:1941` and `:1956` — **before** `validate_request`
runs at `:1570`. Any authenticated client can therefore mint an unbounded set
of fingerprints out of strings that are not valid queries and never execute.

There is a second, independent reason. The normaliser is byte-level, not
AST-level: `MATCH (n:Person)` and `MATCH (x:Person)` produce different
fingerprints, and so do `match` and `MATCH`. So even a well-behaved client with
a fixed set of query shapes produces one fingerprint per *spelling*, and the
cardinality is a property of how the application's query strings are generated
rather than of how many shapes it has.

**Where the premise was wrong, twice.**

The framing — "the one attribute whose safety depends on the workload rather
than on the schema" — is the error. It does not depend on the workload. It is
**attacker-reachable**: reachable by any authenticated caller, deliberately,
with garbage, before validation. A cardinality bound that an authenticated
client can violate on purpose is not a bound, and the distinction between "our
tenants probably won't" and "a tenant cannot" is exactly the distinction §1.4
builds a type to enforce.

The proposed measurement was **also unsound on its own terms**. "Counting
distinct fingerprints per scope over the staging week" would have counted them
over a 5%-sampled trace population, and a distinct-count over a sample
undercounts — worst precisely in the tail, where the rare-fingerprint mass
lives. The experiment was structurally biased toward the answer "it's fine".
A cardinality question can almost never be answered from a sample; that is
worth remembering the next time one is asked.

### 5.3 `error.class` on a dedicated error counter — **yes, build it**

**Decided.** Add an error counter dimensioned by `error.class`, at 10 values.
Three choke points where the error is already in hand and no plumbing is
needed: `src/shard/query.rs:535`, `:4739`, and
`src/client/service.rs:1900`.

**What settled it.** `src/core/error.rs:202` — `GraphError::class` is an
exhaustive match with no `other` arm, and `ErrorClass::Other` is constructed
nowhere. Ten reachable values, not eleven. Ten is affordable; the three choke
points mean the change is a counter increment at sites that already have the
error, not a new error path.

**Where the premise was wrong.** The stated criterion was: "whether the staging
week shows `error.class` being read off logs frequently enough to justify a
counter. If every investigation starts with a log query grouped by class, that
is the signal." **That signal is unobservable.** `error.class` reaches an OTLP
*log record* at 4 of the 20 sites that set it. At the other 16 it is a
`span.record` on a span, and `OpenTelemetryTracingBridge` stamps `trace_id` and
`span_id` onto log records — it does not copy span attributes onto them. So
"group the logs by `error.class`" is a query that would return nearly nothing
regardless of how badly an investigator wanted it, and the absence of that
query in the staging week would have been read as evidence that nobody needs
the counter.

This is a general trap and it is the second instance of it in this section: a
decision criterion phrased as "watch whether people use X" is only valid if X
is *available* to be used. Check that first. Here, the fact that `error.class`
is nearly unqueryable in the logs is an argument **for** the counter, not
against it — it is the strongest one available, and the original criterion had
its sign backwards.

### 5.4 The naming scheme — **`db.*` where semconv exists, `turbolay.*` otherwise**

**Decided.** See §1.9, which holds the full decision, the evidence and the
costs. In brief: the semantic-convention name wherever one genuinely exists,
`turbolay.*` for everything else, chosen for native database-viewer
compatibility.

**What settled it.** A survey of the semantic conventions against all 72
counters. **Two** map: `db.client.operation.duration` (stable) and
`db.client.response.returned_rows` (development). Seventy do not.

**Where the premise was — not wrong, but incomplete.** The original leaned
toward `turbolay.*` throughout, and its **Settled by:** clause was sound: check
whether the backend's database dashboards key off `db.*` names. They do, so the
semconv names earn their inconsistency, and the decision goes against the lean.
What the framing missed is how *little* is being bought and how much it costs
to buy it, and both belong on the record next to the decision:

- Only 2 of 72, and only one of those at stable status. The vocabulary split
  is permanent and buys native rendering for one metric.
- `db.client.operation.duration` is emitted **without `db.namespace`**, which
  semconv marks required-if-applicable. It is applicable — the namespace is the
  `scope` — and it is omitted because `scope` is the unbounded label this whole
  section exists to keep off metrics. Deliberately non-conformant in exactly
  one dimension, and a runbook item, because a conformance checker will flag it
  and the flag will look like a bug.
- And per §1.5, the native latency widget will not light up anyway: the SDK
  cannot produce a histogram data point from a cached snapshot, so the buckets
  go out as an `ObservableCounter` family. The compatibility this decision buys
  is partial. That does not reverse it — the name and `db.system.name` still
  put the series in the right view — but a future reader is entitled to know
  that the thing it was chosen for is only half delivered.

### 5.5 Still open

**BUG-2 — `cluster_for_scope` holding `clusters` across a shard open**
(`src/engine/cluster.rs:1216-1230`). Not fixed, and not fixable mechanically:
releasing the mutex around
`open_promotable_scoped_with_memory_options(...).await` admits two callers
opening the same scope concurrently. The fix needs a one-open-per-scope guard —
an in-flight map keyed by scope, or a per-scope `OnceCell` — and that is a
design with its own failure modes, not a narrowing of a critical section. It is
recorded here rather than in a bug tracker because it interacts directly with
M2: with BUG-2 present, a slow metrics collection and a slow read can share a
cause that has nothing to do with the cache gauges, and M2's measurement will
be read as though it did.

## 6. Explicitly out of scope

- **Changing `/metrics`.** Same conclusion as the prior plan, now with the
  additional reason in §1.4: the two exports should be allowed to differ in
  dimensionality.
- ~~**Adding counters to the kernel.**~~ **No longer out of scope, and this is
  the one scope change the amendment makes.** The original line said §1 exports
  what exists, and that where a counter is missing it is named and left to its
  own change. Two of those changes have now been argued and are in: the
  per-class error counter (§5.3) and the three duration histograms (§1.10, step
  H1). Both are kernel changes to `src/core/metrics.rs` and its call sites.
  What stays out is everything *not* named here — H1 converts three of fifteen
  durations and adds nothing else, and the twelve it declines are listed with
  reasons so that declining them is a decision rather than an omission. The
  per-rung traversal counter of §1.6 is still named-and-deferred.
- **Fixing anything the metrics reveal.** Unchanged from the prior plan.
- **Exemplars.** §1.7, and unlike the prior plan's version of this line, with a
  verified reason rather than a deferral.
- **The `/metrics` `scope` label.** Left as it is, deliberately, and documented
  rather than fixed.
