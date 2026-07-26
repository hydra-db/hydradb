---
title: OpenTelemetry logs and traces via a turbolay-telemetry crate
status: draft-for-review
date: 2026-07-26
branch: Turbolay-V3.5
base_commit: 4e7cb82
tags:
  - observability
  - opentelemetry
  - tracing
  - graph-indexer
  - crate-split
---

# OpenTelemetry logs and traces via a `turbolay-telemetry` crate

Goal: make the read path, the write path and the indexing path each traceable
end to end — including their failures — from a single OTLP-compatible backend,
with `graph-node` and `graph-indexer` distinguishable at a glance.

This plan is about *emission*, not about fixing anything it reveals. The query
planner, the writer lease and the indexer's error handling all have known gaps
tracked elsewhere; the point here is that today none of them can be diagnosed
from a running cluster, only from a code read.

## Sources

The analysis this plan rests on is spread across three places; the following are
the files it was derived from.

**Prior plans in this repo**

- `docs/plans/2026-07-25-cypher-compat-and-query-observability.md` — Step 3
  ("EXPLAIN and slow-query logging") is the read-path half of this plan, written
  before the crate existed to put it in. Its Step 3a threshold-and-full-scan
  rule is adopted verbatim below; its Step 3b `EXPLAIN`-over-Bolt work stays
  where it is and is *not* duplicated here.
- `docs/plans/2026-07-25-rendezvous-placement.md` — the writer-ownership model
  the write-path spans have to describe. Decision 10 (nothing in
  `turbolay-placement` reads a clock) is the precedent for how this crate stays
  testable.
- `docs/plans/2026-07-25-sparse-kernel-backend-consolidation.md` — the kernel
  ladder whose rung selection belongs on the read-path span.

**Design notes under `interactive/`**

- `interactive/inside/02-read-path.html`, `03-write-path.html`,
  `04-delete-path.html`, `05-caching.html` — the phase decomposition each span
  tree below mirrors. Where a span name differs from a chapter's phase name,
  the chapter wins; the point is that the trace reads like the book.
- `interactive/indexer.html` — how `graph-indexer` actually fires: the dirty
  marker watermark, the five-second poll, the CSC build, the content-addressed
  publish and the CAS pointer swap. The indexing span tree is this note's
  W1–W5 widgets turned into spans.
- `interactive/write-routing-problem.html` and `write-routing-placement.html` —
  why a write may reach the wrong node, and what "the duel is bounded rather
  than prevented" means. The `writer.acquire` span exists to make that duel
  visible.

**Bug history**

- `docs/bugs-found-fixed/BFG-006-artifact-generation-race.md`,
  `BFG-013-compiled-generation-ahead-of-read-epoch.md` and
  `BFG-014-unfenced-indexer-generation-gc.md` — the three bugs that a
  `cell_id` + `generation` correlation between the write and indexing paths
  would have surfaced directly.
- `BFG-007-bookmark-and-reader-freshness-contract.md`,
  `BFG-009-epoch-scoped-read-unpinned-composition.md` and
  `BFG-011-wal-tail-visibility-hole.md` — the freshness class. Every one of
  them is a question about `read_epoch` versus what was actually visible, which
  is why `read_epoch` is a mandatory span attribute rather than a nice-to-have.

**Memory**

- `cell-writer-fencing-pingpong` — production `Fenced` errors are three
  graph-nodes ping-ponging one cell's writer, with no single-writer lease. This
  is the single strongest argument for the `writer.acquire` span, and the
  worked example used throughout §4.

**Reference repo**

- `../sleet/src/daemon.rs:458-472` — the fenced-handle re-open delay that
  `src/core/state.rs:380-410` deliberately matches. Worth reading before
  changing anything in `refresh_writer_fence`.

## What we found first

### Tracing today is a facade with nothing behind it

`tracing` is a workspace dependency and is imported in eleven files, but the
usage is entirely flat events — **zero spans, zero `#[instrument]`** across
62k lines of `src/`:

| File | Call sites |
|---|---|
| `src/query/coordination.rs` | 7 |
| `src/bin/graph-node.rs` | 6 |
| `src/bin/graph-indexer.rs` | 6 |
| `src/client/bolt.rs` | 4 |
| `src/bin/graph_node/tls.rs` | 3 |
| `src/engine.rs`, `src/client/http.rs` | 2 each |
| `src/shard/query.rs`, `src/shard/topology_tail.rs`, `src/engine/artifact_build.rs`, `src/client/bolt/values.rs` | 1 each |

`src/shard/query.rs` — 8,385 lines, the entire read path — has exactly one
tracing call. `src/shard/write.rs` — 5,108 lines — has none.

### The two binaries do not log the same way

| | init site | format | spans | identity |
|---|---|---|---|---|
| `graph-node` | `src/bin/graph-node.rs:227` `init_tracing` | `.json()` | **suppressed** | none |
| `graph-indexer` | `src/bin/graph-indexer.rs:44` | plain text `fmt()` | — | none |

Two independent initialisations, two different output formats, and neither
emits a service or binary name. In a shared log sink the only way to tell a
node line from an indexer line is to recognise the message text.

`graph-node.rs:232-233` additionally sets `.with_current_span(false)` and
`.with_span_list(false)`. Adding spans without flipping those two lines would
produce nothing visible — worth stating plainly because it is the kind of thing
that costs a day.

### Counters exist; causality does not

`src/core/metrics.rs:90` `GraphOperationalMetricsSnapshot` already carries 36
operational counters — write attempts/commits/retries, artifact build and
publish durations, GC, verifier runs, query rows started/completed/failed,
GraphBLAS-versus-Rust fallbacks, backpressure waits — served from
`src/bin/graph_node/admin.rs:98`. `graph-indexer` has its own smaller
`IndexerMetrics` (`src/bin/graph-indexer.rs:25`), served from `:308`.

So we can already see *that* queries are slow or writes are retrying. What no
counter can answer is *which* query, on which cell, at which epoch, behind which
other operation. That is the gap, and it is a span-shaped gap.

### The indexer flattens its failures into one string

`run_registered_scopes_cycle` (`src/bin/graph-indexer.rs:124`) and
`run_index_cycle` (:189) both accumulate errors into a `Vec<String>`, then join
them with `"; "` into a single `std::io::Error::other` (:163, :266). The caller
at :106 logs the result as:

```rust
tracing::warn!(error = %error, "graph index cycle failed; retrying")
```

One warn line, one flattened string, N failures from N different cells and edge
types, no cell id, no edge type, no generation, no ready-state transition. A
cycle that fails on one cell out of eight is indistinguishable from one that
fails on all eight. This is the worst observability in the repo and the easiest
to fix, because the structure is already there in the `failures` vector — it is
thrown away at the last moment.

## 1. The crate

`crates/telemetry`, package `turbolay-telemetry`, added to `[workspace] members`
in the root `Cargo.toml`, with every third-party version inherited via
`{ workspace = true }` per the rule already stated there.

### What it owns, and what it deliberately does not

The dependency runs **away** from the kernel, exactly as `turbolay-placement`
does: `turbolay-telemetry` does not depend on `slatedb-graph-kernel`, and the
kernel does not depend on `turbolay-telemetry`. Only the two binaries do.

This works because of how `tracing` is layered. The kernel keeps using the
plain `tracing` facade — `info!`, `warn!`, `#[instrument]` — which is a no-op
when no subscriber is installed and stays free in tests and benchmarks. The
telemetry crate owns the *subscriber* side: how those spans and events become
OTLP. Neither side needs to name the other's types.

The practical payoff is that adding OTel does not touch `[features]` in the root
manifest at all. Every one of the eleven existing feature combinations keeps
compiling unchanged, and `cargo test` never pulls `opentelemetry-*` or `tonic`.

```
crates/telemetry/src/
  lib.rs        init(), TelemetryConfig, the ServiceIdentity type
  layers.rs     fmt + OTLP-trace + OTLP-log layer stack assembly
  redact.rs     the field denylist layer
  sampler.rs    error-and-full-scan-biased sampler
  semconv.rs    the turbolay.* attribute registry as consts
  propagate.rs  W3C traceparent parse/format — no OTel dependency
  error_class.rs GraphError variant -> error.class, behind a feature
```

`propagate.rs` is deliberately dependency-free: W3C traceparent is a 55-byte
ASCII string with a fixed layout, and parsing it needs no OTel types. This
matters for §7 Step 5b, where the kernel has to carry a traceparent across the query
transport without gaining an OTel dependency.

`error_class.rs` is the one place a kernel type is named, so it sits behind an
off-by-default `kernel-errors` feature that pulls `slatedb-graph-kernel` as a
dev-and-binary-only dependency. If that proves awkward, the fallback is to put
the mapping in `src/core/error.rs` as an inherent `GraphError::class()` method
returning `&'static str` — no dependency either way. Decide at implementation
time; the mapping table in §5 is the same in both cases.

### Dependencies

| Crate | Why |
|---|---|
| `tracing`, `tracing-subscriber` | already in `[workspace.dependencies]` |
| `tracing-opentelemetry` | bridges `tracing` spans to OTel spans |
| `opentelemetry`, `opentelemetry_sdk` | tracer provider, resource, sampler |
| `opentelemetry-otlp` | the exporter |
| `opentelemetry-appender-tracing` | bridges `tracing` **events** to OTel logs |
| `opentelemetry-semantic-conventions` | `service.*`, `db.*` attribute names |

All six go behind a single `otlp` feature, default off. With the feature off the
crate still builds and `init()` still works — it just installs the fmt layer
alone, which is what tests and local runs want.

`opentelemetry-appender-tracing` is the piece that makes this a logs *and*
traces plan rather than a traces plan. Every existing `tracing::info!` and
`tracing::warn!` in the codebase becomes an OTLP log record automatically, and
because the appender runs inside the same subscriber it stamps each record with
the enclosing `trace_id` and `span_id`. No second logging API, no call-site
changes, and clicking from a log line to its trace works on day one.

### `init()` and binary identity

Both binaries replace their bespoke setup with one call:

```rust
// src/bin/graph-node.rs
turbolay_telemetry::init(TelemetryConfig::from_env(ServiceIdentity::GraphNode))?;

// src/bin/graph-indexer.rs
turbolay_telemetry::init(TelemetryConfig::from_env(ServiceIdentity::GraphIndexer))?;
```

`ServiceIdentity` resolves to the OTel resource attributes and to a flat log
field:

| | `service.name` | `binary` field | filter env var |
|---|---|---|---|
| `GraphNode` | `turbolay-graph-node` | `graph-node` | `GRAPH_NODE_LOG` |
| `GraphIndexer` | `turbolay-graph-indexer` | `graph-indexer` | `GRAPH_INDEXER_LOG` |

`service.name` is the correct OTel-native way to separate the two, and it is
what the backend groups by. The redundant flat `binary` field exists because
somebody will be tailing a pod with `grep` at 2am and resource attributes are
not in the log line. Both, not one.

Also set `service.version` (from `CARGO_PKG_VERSION` plus the git SHA emitted by
`build.rs`), `service.instance.id` (the pod name, from `POD_NAME` with a
hostname fallback), and `deployment.environment.name`.

Separate `EnvFilter` variables per binary matter more than they look: today both
read `RUST_LOG`, so turning the indexer up to `debug` in a shared chart also
turns up three graph-nodes serving live query traffic. Each falls back to
`RUST_LOG`, then to `info`.

Both binaries get **JSON**, and `graph-node.rs:232-233` flips to
`.with_current_span(true).with_span_list(true)`.

### Configuration surface

| Env var | Default | Meaning |
|---|---|---|
| `OTEL_EXPORTER_OTLP_ENDPOINT` | unset | unset ⇒ fmt layer only, no exporter |
| `OTEL_EXPORTER_OTLP_HEADERS` | unset | auth for the collector |
| `OTEL_EXPORTER_OTLP_PROTOCOL` | `http/protobuf` | see §7 |
| `OTEL_TRACES_SAMPLER_ARG` | `0.05` | head ratio for uninteresting traces |
| `GRAPH_SLOW_QUERY_MS` | `1000` | read-path warn threshold |
| `GRAPH_NODE_LOG` / `GRAPH_INDEXER_LOG` | `RUST_LOG`, else `info` | per-binary filter |
| `POD_NAME` | hostname | `service.instance.id` |

Standard `OTEL_*` names where OTel defines one — the collector sidecar sets
those already and nobody should have to learn a Turbolay-specific spelling.

**Unset endpoint means no exporter.** Tests, `cargo run`, and the examples must
not need a collector, must not block on startup, and must not print a connection
error every five seconds.

## 2. The attribute registry

One table, in `semconv.rs`, used by all three paths. Consistency is what makes
cross-path correlation a backend query rather than an archaeology exercise.

| Attribute | Type | On | Notes |
|---|---|---|---|
| `turbolay.scope` | string | all | graph scope / tenant root |
| `turbolay.cell_id` | string | all | **the** join key across paths |
| `turbolay.node_id` | string | all | which graph-node |
| `turbolay.read_epoch` | u64 | read | the epoch the read was pinned to |
| `turbolay.commit_epoch` | u64 | write | epoch returned by the commit |
| `turbolay.generation` | u64 | index, read | compiled artifact generation |
| `turbolay.base_sequence` | u64 | index | generation's base storage sequence |
| `turbolay.edge_type` | string | index, write | |
| `turbolay.query.fingerprint` | string | read | shape hash, parameters elided |
| `turbolay.query.access_path` | string[] | read | per pattern, from `RowQueryPlan` |
| `turbolay.query.optimizer_passes` | string[] | read | |
| `turbolay.query.rows_estimated` | u64 | read | |
| `turbolay.query.rows_returned` | u64 | read | |
| `turbolay.query.full_scan` | bool | read | the alarm bit; see §3 |
| `turbolay.kernel` | string | read | which rung of the sparse-kernel ladder |
| `turbolay.writer.epoch` | u64 | write | SlateDB writer epoch held |
| `turbolay.writer.retries` | u64 | write | |
| `turbolay.consistency` | string | read | `ClientReadConsistency` |
| `turbolay.correlation_id` | string | all | caller-supplied; see §7 Step 5a |
| `turbolay.caller.step` | string | all | caller-supplied operation label |
| `error.class` | string | all | §5 |

Plus `db.system.name` = `neo4j` on client-facing spans. Turbolay speaks Bolt,
and the semconv value is what makes an APM's generic database view work without
per-vendor configuration — it describes the wire protocol, not a claim about
the implementation.

**Cardinality.** `cell_id`, `edge_type`, `node_id`, `access_path` and `kernel`
are all bounded and safe. `query.fingerprint` is bounded by the number of
distinct query *shapes*, which for a fixed application is small — this is
precisely why it is a fingerprint and not the query text. `scope` is unbounded
in principle since it is per tenant; acceptable on spans, and it is the reason
§8 defers mirroring these onto metric labels.

`correlation_id` is unbounded by construction — one value per inbound request.
It is a span attribute and a log field and **must never become a metric label**;
it belongs in the same "spans only" bucket as `scope`, more emphatically. Its
whole value is that it is high-cardinality: it identifies one request.

**Never recorded, at any level:** query parameter values, property values,
vertex or edge property maps, bearer tokens, bookmarks. §5 makes this
structural rather than a convention.

## 3. Read path

Root span opens at the service boundary, in `src/client/service.rs:883`
`execute_rows` and `:978` `execute_page`, which both Bolt and HTTP funnel
through.

```
client.query                      service.rs:883 / :978
├─ query.admission                service.rs:764  effective_runtime_limit_ms
├─ query.bookmark_wait            service.rs:826  ensure_bookmark        [conditional]
├─ query.parse                    query/opencypher.rs:263
├─ query.plan                     shard/query_optimizer.rs:19  explain_row_query_plan_with_stats
├─ query.execute                  shard/query.rs
│  ├─ storage.snapshot            core/snapshot.rs
│  ├─ artifact.lookup             engine/artifact_build.rs:531  latest_matrix_artifact
│  ├─ kernel.expand               sparse_kernel/graphblas.rs                  [per hop]
│  └─ storage.wal_tail            shard/topology_tail.rs
└─ query.page                     client/bolt.rs                             [per PULL]
```

Notes on the three spans that are doing real work here:

**`query.plan`** is the highest-value span in the whole plan. The planner
already computes everything needed — `explain_row_query_plan_with_stats` builds
a complete `RowQueryPlan` — and then `grep -rn "RowQueryPlan" src/client/`
returns nothing, so it never leaves the shard. Attaching `access_path`,
`optimizer_passes` and `rows_estimated` to this span is almost free and is what
turns "a query timed out" into "a query timed out on `FullEdgeScan`".

**`query.page`, one per PULL.** `remaining_bolt_runtime_ms`
(`src/client/bolt.rs:1362`, called at :1319) recomputes the remaining budget on
every PULL against a fixed deadline. That is why the staging failure reported
`29999 ms; limit is 29999 ms` rather than 30000 — a detail that took a code read
to explain and that a span-per-PULL makes self-evident. It also shows where a
slow query actually spends its time when the client is paging lazily.

**`query.bookmark_wait`** only appears when a bookmark forces a wait. Its
duration is read-your-writes latency, which is otherwise unmeasurable, and it is
the first thing to look at for the BFG-007 class of complaint.

### Naming the query

`turbolay.query.fingerprint` has to be built; nothing existing substitutes for
it. `ClientQueryRequest` already carries a `query_id`
(`src/client/service.rs:361`), but the Bolt path generates it as
`{session_id}-query-{n}` (`src/client/bolt.rs:551-553`) — it is a
cancellation handle, not an identity. It is not stable across sessions, means
nothing to a caller, and two runs of the same statement never share one. Any
attempt to correlate on it will look like it works in a single-session test and
fail in production.

So the fingerprint is a hash of the **query shape** with parameters elided,
computed once at parse and cached alongside the existing parsed-query cache
(`src/query/opencypher.rs:321`). Two properties are load-bearing: it is stable
across runs, so "this shape got slower" is a query a backend can answer; and it
contains no parameter values, so it is safe to log unredacted.

Fingerprint answers *which statement*. It deliberately does not answer *which
caller invocation* — that is `turbolay.correlation_id`, §7 Step 5a. Both are
needed, and neither substitutes for the other: the staging delete failure needs
the fingerprint to say "the RELATES delete" and the correlation id to say "the
one for source 693cdeab".

### Slow-query and bad-plan logging

Adopted from Step 3a of the cypher-compat plan. At `query.plan` close, emit a
structured `WARN` when **either** holds:

1. elapsed exceeds `GRAPH_SLOW_QUERY_MS` (default 1000), **or**
2. the chosen plan contains `RowQueryOptimizerPass::FullScanFallback`,
   `RowQueryAccess::FullEdgeScan`, or `RowQueryAccess::AllVertexScan` —
   regardless of elapsed time, and set `turbolay.query.full_scan = true`.

The second condition is the one that pays. It catches a bad plan on a small
graph *before* it becomes a timeout on a large one, which is exactly the failure
staging hit. A full-scanning query that returns in 3ms today is a 30-second
timeout after the tenant grows, and only rule 2 sees it coming.

## 4. Write path

Root span at the mutation entry in `src/client/service.rs`; the routed-cluster
entry points are the six `ensure_local_writer` call sites in
`src/query/coordination.rs` (:3161, :3187, :3213, :3230, :3258, :3286).

```
client.mutate                     client/service.rs
├─ writer.acquire                 engine/cluster.rs:434  ensure_local_writer
│  ├─ placement.resolve           crates/placement  rendezvous owner()
│  ├─ writer.authority            shard/lifecycle.rs:403  ensure_write_authority
│  ├─ writer.promote              shard/lifecycle.rs:419  promote_to_writer   [conditional]
│  └─ writer.fence_refresh        core/state.rs:389  refresh_writer_fence
├─ shard.write_txn                shard/write.rs:2387 write_edge_txn / :3217 delete_edge_txn
│  ├─ write.index_update          shard/write.rs
│  └─ storage.commit              slatedb
└─ write.bookmark                                    returns commit_epoch
```

**`writer.acquire` is why this section exists.** Per the
`cell-writer-fencing-pingpong` memory, production `Fenced` errors are three
graph-nodes ping-ponging one cell's writer with no single-writer lease. Today
that appears as sporadic errors with no way to see the pattern. As a span tree
it becomes obvious: three traces, three different `turbolay.node_id`, same
`turbolay.cell_id`, `turbolay.writer.epoch` climbing on every acquisition.

The raw material is already in place. `src/core/state.rs:441`
`log_fence_attribution` reads the advisory cell-writer record and logs
`last_promoted_by`, `last_promoted_epoch` and `last_promoted_at` — a genuinely
good diagnostic that currently lands as a free-floating warn with nothing to
correlate it to. Promote those three to attributes on `writer.fence_refresh`
and the ping-pong is a single backend query: group fence events by `cell_id`,
count distinct `last_promoted_by` over five minutes.

**`writer.promote` is conditional and should stay a distinct span.** The
don't-promote rule from the rendezvous-placement note is a correctness
invariant; a span that materialises only when a promotion actually happens makes
violations countable instead of theoretical.

**`shard.write_txn` needs `turbolay.writer.retries`.** The kernel already
counts `write_attempts`, `write_commits` and `write_retries` in
`GraphOperationalMetricsSnapshot`, so the retry loop is instrumented in
aggregate — it just cannot be attributed to a specific mutation. A retry count
on the span closes that.

## 5. Indexing path

This one is a trace **root** in `graph-indexer`, with no client parent, and it
is where the current state is worst.

```
index.cycle                       graph-indexer.rs:84   the poll loop, one per tick
├─ index.scope_discovery          graph-indexer.rs:132  scope_directory.list()
└─ index.scope                    graph-indexer.rs:134                      [per scope]
   ├─ index.scope_has_data        graph-indexer.rs:167
   ├─ index.cluster_open          graph-indexer.rs:138  open_cells_scoped
   ├─ index.cell                  graph-indexer.rs:196                      [per cell]
   │  ├─ index.refresh_sequence   graph-indexer.rs:201
   │  ├─ index.discover_dirty     graph-indexer.rs:205  dirty_graph_index_edge_types
   │  └─ index.edge_type          graph-indexer.rs:212              [per dirty edge type]
   │     ├─ index.read_current    graph-indexer.rs:213  current_graph_index
   │     ├─ artifact.build        graph-indexer.rs:226  build_graph_index
   │     ├─ artifact.publish      engine/artifact_build.rs:17  build_matrix_tiles
   │     └─ artifact.gc           graph-indexer.rs:239  gc_graph_index_generations
   └─ index.cluster_close         graph-indexer.rs:156
```

Three things this fixes, all of them present in the code today as thrown-away
structure:

**Failures stop being a flattened string.** `run_registered_scopes_cycle` and
`run_index_cycle` build a `Vec<String>` of failures and join it with `"; "` at
:163 and :265. Every element of that vector already knows its scope, cell and
edge type — it is stringified and merged precisely at the moment that context
would become useful. With per-level spans, each failure is recorded on the span
that produced it and the flattening becomes a display concern rather than a data
loss.

**The skip is as interesting as the build.** The `continue` at :224 — when
`current.base_sequence >= dirty_sequence`, so the generation is already
current — is the normal case and is currently invisible. An `index.edge_type`
span that closes with a `skipped` outcome tells you the indexer is healthy and
idle; the absence of any span at all tells you nothing. Distinguishing
"nothing to do" from "not running" is most of what an indexer needs to report.

**Readiness transitions get a cause.** `metrics.ready` flips to `false` at :105
on any cycle failure and back at :101, and the flip is what a Kubernetes probe
acts on. Recording the transition as an event on `index.cycle`, with the failing
cell attached, connects a pod going unready to the specific cell that caused it.

`artifact.publish` should carry the content hash and the CAS pointer outcome
described in `interactive/indexer.html`, and `artifact.gc` should carry the
delete count — the BFG-014 unfenced-GC failure mode is visible as a GC span on a
cell whose writer epoch moved underneath it.

### Correlating the three paths

The indexer has no client parent, so parent-child propagation cannot connect a
write to the indexing that consumes it. Two options:

**Adopted: correlate by attribute.** `turbolay.cell_id` plus
`turbolay.generation` / `turbolay.base_sequence` / `turbolay.read_epoch` on all
three paths, so the join is a backend query — "every span touching `cell-7` at
generation 412" returns the write that dirtied it, the index cycle that
compiled it, and the reads that consumed it, across two services. Costs nothing
beyond the attribute discipline of §2 and directly answers the BFG-006 /
BFG-013 / BFG-014 question, which is always "did these three agree on a
generation".

**Deferred: span links.** Stamping the committing trace id into the artifact
metadata would let `artifact.build` emit a proper OTel span link back to the
write that caused it. Strictly better — it survives clock skew and gives one
clickable edge — but it changes a persisted storage format, which is not
something to do for telemetry alone. Revisit if the attribute join proves
insufficient in practice.

## 6. Failures

`src/core/error.rs` already has a well-shaped taxonomy. It just never reaches a
log in structured form — errors surface as `%error` display strings, so a
backend cannot group them. Add an `error.class`:

| `error.class` | `GraphError` variants |
|---|---|
| `contention` | `ConditionalWriteConflict` (:11), `IdempotencyConflict` (:28), `ControlMetadataConflict` (:35), `RetryExhausted` (:41) |
| `fencing` | `WriteRequiresWriter` (:73), `NotCellWriter` (:89), `UnknownShard` (:63), `CellDropped` (:96) |
| `freshness` | `SnapshotAhead` (:57), `SnapshotExpired` (:103), `SnapshotChanged` (:112), `QueryStatsSnapshotChanged` (:122), `ControlWatermarkRegression` (:48) |
| `admission` | `AdmissionRejected` (:129), `QueryTimeout` (:147) |
| `query` | `QueryParse` (:140), `UnsupportedQuery` (:153), `MissingQueryParameter` (:145) |
| `authz` | `GraphScopeMismatch` (:65), `GraphScopeAccessDenied` (:67) |
| `corruption` | `CorruptValue` (:26), `InvalidKeyComponent` (:21) |
| `config` | `UnsafeDurabilityConfig` (:16) |
| `storage` | `Slate` (:7), `ObjectStore` (:9) |
| `kernel` | `SparseKernel` (:135) |

Three rules matter more than the table:

**Record the error on the innermost span that produced it.** A `Fenced` that
surfaces at `client.mutate` tells you a write failed; the same error recorded on
`writer.fence_refresh` tells you why. Use `span.record_error` at the raising
site and let it propagate as a failed parent status, rather than logging once at
the top.

**Bias the sampler toward failure.** A 5% head sample that drops the one trace
containing the error is worse than useless. The `sampler.rs` policy is:
always keep a trace that contains a non-`Ok` span status; always keep one with
`turbolay.query.full_scan = true`; always keep any `writer.promote` or
`writer.fence_refresh` span; head-sample everything else at
`OTEL_TRACES_SAMPLER_ARG`. Full head-based tail-sampling belongs in the
collector, not the process — but these four rules are cheap locally and cover
the cases that actually get looked at.

**`contention` and `fencing` are expected, not alarming.** Retries are normal.
The class exists so a dashboard can chart the *rate* and alert on a change in
it, not so every occurrence pages someone. Worth writing into the runbook at the
same time, or the first week of data will generate noise that discredits the
whole effort.

### Redaction is a layer, not a convention

`redact.rs` installs a `tracing_subscriber::Layer` that drops or hashes fields
by name before any exporter sees them: `parameters`, `params`, `properties`,
`property_value`, `token`, `bearer`, `authorization`, `password`, `bookmark`.

Putting this in a layer rather than trusting call sites is the whole point. A
denylist that lives in one file is auditable and testable; a rule that lives in
50 call sites is a matter of hope, and the failure mode is tenant data in a
third-party observability backend. Unit-test the layer directly with a
deliberately leaky span.

## 7. Sequencing

Each step lands independently and is useful on its own.

### Step 1 — the crate, and one log format

`crates/telemetry` with `init()`, `ServiceIdentity`, the fmt and OTLP-log
layers, and the redaction layer. Both binaries switched. No spans yet.

Deliverables: `graph-node` and `graph-indexer` both emit JSON carrying
`service.name`, `binary`, `service.version` and `service.instance.id`; separate
`EnvFilter` env vars; `graph-node.rs:232-233` flipped to emit spans;
`OTEL_EXPORTER_OTLP_ENDPOINT` unset behaves exactly as today.

**Done when** a staging log query can filter to one binary by field rather than
by message text, and the existing `tracing::info!` calls arrive at the collector
as OTLP log records.

### Step 2 — read path

The §3 span tree, the attribute registry, and the slow-query / bad-plan warn
rule. `query.plan` first — it is the highest information per line of change,
since the planner output already exists and is simply discarded.

**Done when** a full-scanning query in staging produces a WARN naming its access
path, and the trace shows plan-versus-execute time separately.

### Step 3 — write path

The §4 span tree. `writer.acquire`, `writer.promote`, `writer.fence_refresh`,
and promoting the three `log_fence_attribution` fields to span attributes.

**Done when** the writer ping-pong from the `cell-writer-fencing-pingpong`
memory is a single backend query — distinct `last_promoted_by` per `cell_id`
over a window — rather than an inference from scattered errors.

### Step 4 — indexing path

The §5 span tree, the failure destructuring, the skip outcome, and the readiness
transition event. Independent of Steps 2 and 3; could run in parallel if someone
else picks it up.

**Done when** a partially failing index cycle names the specific scope, cell and
edge type that failed, and a healthy idle indexer is distinguishable from a
stopped one.

### Step 5a — caller correlation

Everything above makes a Turbolay span self-describing. None of it connects that
span to the caller's log line. The staging delete failure is the worked example:
`Error deleting source 693cdeab... after 30.28s` on the ingestion side, and —
once Step 2 lands — a `FullEdgeScan` WARN on the Turbolay side, with no field in
common. `delete_source_from_kg` runs eight steps, several of them looping over
500-item batches, so matching on timestamp plus tenant is guesswork.

**cortex-ingestion already has the identifier.** `X-Correlation-ID` is read at
the edge (`app.py:255`), passed through `sanitize_correlation_id`
(`utils/logging_utils.py:676` — printable-ASCII only, length-bounded, UUID4 when
absent or invalid), bound into a ContextVar by `bind_context`
(`utils/logging_utils.py:641`, mirrored as `current_correlation_id` in
`utils/langfuse_tracing.py:64`), echoed back on the response (`app.py:280`), and
carried into every graph workflow as `params.correlation_id`
(`temporal/workflows/graph.py:93`, `:189`, `:254`). It reaches `utils/graph/`
already. It simply stops at the driver.

**The transport already exists too.** Bolt `tx_metadata` is parsed at
`src/client/bolt.rs:1160` — and line 1162 reaches straight into the dict for
`turbolay.consistency` and discards every other key. On the other side,
`core/db/graph/turbolay.py:467` already constructs
`Query(query, metadata={"turbolay.consistency": ...})`. The channel is live in
production today and carries exactly one key.

The work, then, is small on both sides:

1. **Turbolay** — replace the single-key lookup at `bolt.rs:1160-1168` with an
   allowlisted read of the whole `tx_metadata` dict: `turbolay.consistency`
   (existing behaviour, unchanged), `turbolay.correlation_id`,
   `turbolay.caller.step`, and later `traceparent` for Step 5b. Carry them on
   `ClientQueryRequest` and stamp them onto the root span, so they inherit to
   every child.
2. **cortex-ingestion** — set `turbolay.correlation_id` from the ContextVar and
   `turbolay.caller.step` from the operation label already passed to
   `_with_heartbeat` (e.g. `delete_source.delete_relates_batch_3_0`). That label
   is currently used only for Temporal heartbeats and is exactly the
   step-identity string the join needs.

Two things that will bite if they are not designed in from the start:

**Metadata is currently sent on reads only.** `turbolay.py:463-468` builds the
`Query(...)` wrapper inside `if read_only:` and passes the bare query string
otherwise. Every mutation — including the delete that started all of this —
carries no `tx_metadata` at all today. The wrapper has to move out of that
branch, or Step 5a lands and the one path we most want to correlate is the one
path still unlabelled.

**`tx_metadata` is untrusted input.** It arrives from any Bolt client and would
become both a span attribute and a log field. Validate on the Turbolay side
rather than trusting the caller's sanitiser: bounded length, printable ASCII,
reject rather than truncate, and drop unknown keys instead of forwarding them.
This mirrors `sanitize_correlation_id` deliberately — with the difference that
Turbolay **drops** an invalid value where the Python generates a fresh UUID.
Turbolay must not mint a correlation id; a server-invented one that matches
nothing upstream is worse than an absent field, because it looks like a join key
and is not. Route the whole dict through `redact.rs` regardless.

**This does not touch a wire format** — `tx_metadata` is standard Bolt and the
dict is already being parsed. So unlike Step 5b it can move earlier: it is
useful the moment Step 2 exists, and it is what turns the read-path WARN from
"some query full-scanned" into "the RELATES delete for source 693cdeab
full-scanned". Sequence it immediately after Step 2 if the delete timeouts are
still live.

**Done when** a `FullEdgeScan` WARN in the Turbolay logs and an
`Error deleting source ...` line in the ingestion logs can be joined on
`correlation_id` with no timestamp arithmetic.

### Step 5b — cross-node propagation

Carry a W3C `traceparent` on the query transport frame so a distributed query
(`src/query/coordination.rs:2813` `execute_distributed_query_plan`, `:2755`
`execute_cypher_rows_many`) is one trace rather than one per node. The kernel
treats it as an opaque `Option<String>`; `propagate.rs` does the parsing.

Note this is a genuinely different problem from 5a and neither replaces the
other. 5a is ingestion → Turbolay across a process and an organisation boundary,
over a channel that already exists. 5b is Turbolay-node → Turbolay-node over the
internal query transport, and is the only step in this plan that changes a wire
format. A `correlation_id` propagated by 5a does ride along the same internal
transport for free once 5b carries arbitrary context, which is an argument for
doing 5b eventually — not for delaying 5a until it.

Last because a single-node trace is already most of the value. It is also the
step that cannot be retrofitted cheaply later — the frame change is easier to
make once than twice, so if the transport frame is being revised for any other
reason, pull this forward to ride along.

## 8. Open decisions

**OTLP transport: `http/protobuf` or gRPC.** Recommending `http/protobuf` — it
avoids a `tonic` dependency and its transitive `hyper`/`tower` stack, it
traverses proxies and service meshes without configuration, and every collector
accepts it. gRPC is somewhat more efficient at high span volume, which is not
our constraint. Confirm what the staging collector actually exposes before
committing; the choice is one feature flag either way.

**Where `error.class` lives.** Either the off-by-default `kernel-errors`
feature on `turbolay-telemetry`, or an inherent `GraphError::class()` in
`src/core/error.rs`. The second is simpler, keeps the mapping next to the
variants it maps so a new variant is an obvious omission, and adds no
dependency — mild preference for it, but it does put a telemetry concept in the
kernel. Either is defensible.

**Metrics.** Out of scope here, and deliberately: `GraphOperationalMetricsSnapshot`
plus the admin `/metrics` endpoint already work, and OTel metrics is a separate
export pipeline with its own cardinality trap — `turbolay.scope` is unbounded
and must not become a metric label. If it happens later, the shape is a periodic
callback reading the existing snapshot into an OTel meter, leaving `/metrics`
untouched.

**Sampling defaults.** 5% head with the four always-keep rules is a starting
guess, not a derived number. Revisit once the actual span volume from a
three-node staging cluster is known — it may well be low enough to sample
everything.

## 9. Explicitly out of scope

- **`EXPLAIN` over Bolt** — Step 3b of
  `docs/plans/2026-07-25-cypher-compat-and-query-observability.md`, still owned
  there. This plan makes the planner output visible in traces; that one makes it
  queryable by a client. Complementary, not overlapping.
- **Fixing anything the traces reveal.** Predicate pushdown, the cost-model
  fallback inversion, the missing single-writer lease and the indexer's
  error handling all have their own tracked work. Emission only.
- **Continuous profiling / span metrics / exemplars.** All reasonable later;
  none of them are the current gap.
- **Trace-driven alerting.** Needs a baseline first. Ship Steps 1–4, watch a
  week of staging, then write alerts against what is actually noisy.
