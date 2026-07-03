---
title: "RFC 0017: Observability — SLIs, Metrics & the Bottleneck Ledger"
status: draft
date: 2026-07-03T00:00:00Z
related:
  - 0000-rfc-index.md
  - 0001-strong-consistency-model.md
  - 0004-graph-data-model-and-write-path.md
  - 0005-posting-list-substrate.md
  - 0006-index-framework.md
  - 0007-opencypher-read-path.md
  - 0008-http-service-and-fleet.md
  - ../plan.md
---

# RFC 0017: Observability — SLIs, Metrics & the Bottleneck Ledger

## Summary

RFC 0000's entire optimization posture rests on one sentence: *"Optimizations are deliberately deferred until the running v0 system produces measurable numbers on real S3... CSR adjacency, WCOJ/leapfrog joins, and bitpacked frames become later RFCs and run on data, not intuition."* This RFC is where that data comes from. It specifies, exactly:

- the **user-facing SLIs** for the public API surface — the JSON upsert write path and the openCypher read subset (RFC 0007/0008), framed around the D12 reference workload (a RAG knowledge graph: `Source → Chunk → Entity`, `RELATES` edges, 1–10M nodes);
- the **measurement spine**: named probe points at every seam the RFC series already defines (write batch fan-out, index build-loop tick, posting-block RMW, freshness gate, anchor/index lookup, per-hop adjacency read + intersect, variable-length BFS frontier expansion, changelog-tail scan, tail-merge, node fetch), so a slow *anything* decomposes into exactly one guilty phase in one look;
- the full **metric inventory** per subsystem — name, type, labels, and, for each metric, *which usual suspect it convicts or exonerates* — with the graph-specific counters (per-hop frontier sizes and N+1 neighbor fan-out) called out as first-class, because on an S3-backed graph the neighbor fan-out **is** the dominant traversal cost;
- the **usual-suspects ledger**: symptom → candidate causes → the discriminating metrics, written down *before* the first incident, not during it;
- the **optimization before/after matrix**: every deferred optimization in RFC 0000's correct-first ledger mapped to the primary metric that must move and the guardrail metric that must not — RFC 0009+ is **gated** on these baselines existing, **measured on real S3**;
- **cardinality and naming rules** (what may never become a label), the slow-query log, per-query `debug: true` stats, and a set of should-be-zero **invariant counters** that turn "structurally impossible" claims from RFCs 0004–0007 into monitored facts;
- a **phased rollout** aligned to milestones M0–M3, most essential first.

The one hard rule that overrides convenience: **benchmark on real S3, never LocalStack-only** (D12, §5). This RFC touches no data formats and no locked contracts. Everything here is instrumentation layered on the code paths RFCs 0003–0008 already specify; the one (optional, Phase 2) durable addition — an ops heartbeat key under `m/` — is explicitly flagged as an RFC 0003 keyspace registration.

## Decision

1. **Instrument at the seams the RFCs define, not ad hoc.** Every phase named by RFC 0004 (write fan-out steps), RFC 0005 (posting split/rollup RMW), RFC 0006 (build-loop tick steps), and RFC 0007 (read-path phases) gets a timer with exactly that RFC's phase name. The metric taxonomy *is* the RFC taxonomy — no second vocabulary to translate in an incident.
2. **The `metrics` facade crate + a Prometheus pull exporter on the internal/admin plane (RFC 0008).** Recording sites depend only on the facade; the exporter is swappable (OTLP later) without touching engine code.
3. **An instrumented `ObjectStore` wrapper is the ground truth (D1-safe).** turbolay constructs the `object_store` handle it hands to SlateDB (via `common::StorageBuilder`, D8), so it wraps that handle — **no SlateDB fork, no upstream patch** (D1). Every S3 GET/PUT/DELETE/LIST/HEAD is counted, timed, and byte-metered there. On S3 the scarce resources are **requests, latency, and atomicity — not bytes**: GETs and PUTs are priced and latency-bounded per *request*, so request count and per-request latency are the ground truth this wrapper exists to capture. It works regardless of what SlateDB's own stats surface exposes.
4. **One `QueryStats` / `WriteStats` / `TickStats` struct per operation, emitted once at completion.** Phase timers and work counters accumulate into a plain struct threaded through execution; metrics, the slow-query log, and the `debug: true` response all render from that single struct at a single emission point — no partial emission on error paths, no double counting.
5. **Cardinality budget is a hard rule**: `namespace`, `predicate` (bounded by the schema/registry), `index` (id+kind, bounded by the registry), operation/phase/outcome enums are legal labels. UIDs, xids, tokens, seq values, property values, and per-node degrees are **never** labels. Unbounded-cardinality curiosity (hot supernodes, big frontiers, hot predicates) is served by sampled structured log events, not metric series.
6. **Invariant counters**: violations of "structurally impossible" properties (out/in projection asymmetry, orphaned index entries, xid→uid mapping misses, changelog gaps, watermark overshoot, posting-order violations) each increment a `turbolay_invariant_violations_total{kind}` counter that alerts at ≥ 1. In v0 these are the cheapest correctness monitors we will ever buy — and they directly guard the bidirectional-storage and index-consistency contracts D6/D10 make load-bearing.
7. **Phased, essential-first** (§8): Phase 0 (facade + **object-store wrapper** + **write-path fan-out timers** + **invariant counters**) lands with M0/M1 code; Phase 1 (the full metric matrix: watermark lag, tick breakdown, split counts, query phase timers, per-hop frontier sizes, N+1 fan-out, tail entries, deleted-bitmap cardinality) is an M2 exit criterion; Phase 2 (fleet + HTTP RED + slow-query log + `debug: true`) lands with M3; Phase 3 (**benchmark-grade counters, captured on real S3**) must be live **before RFC 0009 starts** — no baseline, no optimization work.

## Motivation

Three forces converge here:

1. **The correct-first ledger needs numbers — measured on real S3.** RFC 0000 defers CSR adjacency (0009), bitpacked frames + block-max WAND (0010), and vacuum (0012), each with a trigger phrased as a measurement ("traversal latency measured too high on real S3," "posting decode dominates profiles," "deleted-bitmap cardinality growth"). None of those triggers can fire, and no before/after claim for the eventual fix can be honest, unless the corresponding metric exists **before** the optimization ships, is unchanged across it, and was **baselined against real S3** — because the thing an S3-backed graph is fighting is cold first-hop latency, and that is precisely what an in-memory or LocalStack harness cannot reproduce (§5). §5 makes this mapping exhaustive and binding.
2. **The architecture has known usual suspects.** `plan.md` §8's correct-first ledger and the prior-art notes already name them: N+1 neighbor fan-out on traversal (RFC 0007 calls it "the dominant cost"), posting-list RMW write amplification in the builder, tail-scan latency when the builder lags, deleted-bitmap growth, reader freshness waits, and **cold vs warm first-hop S3 latency**. This RFC is where each suspect gets its metric, so that when p99 degrades the question "which suspect is it?" is answered by reading a dashboard, not by re-deriving the system from the RFCs.
3. **The public API defines what "slow" means.** The workload we build for (D12) is a RAG knowledge graph: upsert nodes/edges via JSON, then openCypher reads like `MATCH (s:Source)-[:HAS_CHUNK]->(c:Chunk)-[:MENTIONS]->(e:Entity) WHERE e.name = $x RETURN ...` — anchor on a value index, then expand adjacency posting lists hop by hop, intersecting with filters. Every SLI in §1 is stated in terms a user of that API experiences (`ack.seq` latency, query latency by consistency mode, `reader_behind` frequency, rows returned); internal metrics exist to *explain* movements in those SLIs, not as an end in themselves.

The system is single-writer, changelog-driven, and S3-backed — which means nearly every performance mystery reduces to one of: an S3 round trip that shouldn't be there (a cold posting-list part, a cold node fetch), a phase doing more units of work than expected (frontier UIDs, adjacency parts, postings decoded, tail entries, nodes fetched), or a producer (builder / reader-replay) lagging its consumer. The metric inventory is designed so each of those three families is directly countable — and the graph-specific twist is that traversal turns *one* logical hop into *frontier-many* neighbor reads, so per-hop frontier size and N+1 fan-out are the metrics that separate "the query is inherently big" from "we are doing S3 round trips we could have batched."

## 1. User-facing SLIs (the public API surface)

These are the numbers a user of the graph API would ask about, and the only numbers that justify everything else in this RFC. All are per-namespace.

| SLI | Definition | Metric source | Graph-workload note |
|---|---|---|---|
| **Write ack latency** | `upsert`/`delete` call → `{ok, seq}` ack, p50/p99 | `turbolay_write_ack_duration_seconds` | Dominated by the atomic `WriteBatch` PUT + `flush_interval` (~50–150ms S3 Standard). Note: an edge is a *fan-out* — node record + `EdgeOut` + `EdgeIn` + affected indexes + changelog + `m/latest_seq` in one batch (RFC 0004). The before/after metric for the S3 Express WAL decision. |
| **Query latency** | `query().fetch()` → response, p50/p99, **by consistency mode** (`none`/`session`/`strict`) and **by hop depth** | `turbolay_query_duration_seconds{consistency,max_hops}` | Separating modes matters: session-token queries include gate wait. Separating hop depth matters: a 1-hop lookup and a `*1..3` variable-length traversal are different animals and blending them hides fan-out blowups inside "queries got slow." |
| **Read-your-writes freshness** | time a session-token query spends blocked in the freshness gate | `turbolay_query_phase_duration_seconds{phase="gate"}` | The RAG pattern "upsert a chunk + its edges → immediately query the subgraph" (`Consistency::session(ack.seq)`) lives or dies on this. |
| **Retryable-error rate** | `reader_behind` + `index_behind` per second, and their retry-until-success latency tail | `turbolay_query_requests_total{outcome=...}` | Both are designed, load-shedding signals (RFC 0001/0007) — but their *rate* is the user-visible symptom of reader replay or builder lag. |
| **Hard-error rate** | `unindexed_property`, `malformed_cypher`, `unsupported_cypher`, `oversize_node`, 5xx | same counter | `unindexed_property` spikes usually mean a client shipped a `WHERE` before the index was created — an integration bug worth seeing (RFC 0006 default: unindexed predicate errors unless brute-force is opted in). |
| **Result size** | rows returned and nodes materialized per query, p50/p99 | `turbolay_query_rows_returned`, `turbolay_query_nodes_fetched` | A traversal can return few rows but fetch a huge frontier; both numbers are needed to reason about cost, and both feed the N+1 story below. |
| **Index freshness** | worst per-index watermark lag (seconds and entries) | `turbolay_index_watermark_lag_*` | Invisible to correctness (the changelog-tail merge hides it, D4) but the direct driver of tail-phase cost and `index_behind`. |
| **Cold first-hop latency** | first-hop adjacency + anchor latency split by cache warmth | `turbolay_query_first_hop_duration_seconds{warmth=cold\|warm}` | **The real enemy for an S3-backed graph** (D12, §5). A cold anchor/first-hop pays a full S3 GET the warm path serves from block cache; this is the number LocalStack cannot reproduce and the primary target of the CSR/locality work in RFC 0009. |

## 2. The measurement spine

Numbered probe points over the end-to-end pipeline. Every metric in §3 attaches to exactly one probe.

```
  client ──HTTP──▶ [P1 service] ──▶ [P2 write fan-out]──────────────▶ ack{seq}
                                      │  encode node → encode
                                      │  EdgeOut/EdgeIn posting lists →
                                      │  index fan-out → WriteBatch
                                      │  commit → m/latest_seq ──────▶ [P0 object store: PUT]
                                      ▼
                                 l/{seq} changelog
                                      │
                        [P3 build-loop tick]  registry → scan tail → fetch node →
                                      │        extract → apply(RMW) → commit(+m/wm)
                                      │            │
                                      │       [P4 posting substrate]
                                      │        locate part → merge? → append → split(512KiB)?
                                      ▼
                              m/wm/{index} watermark
   ────────────────────────────────────────────────────────────────────────
   client ──HTTP──▶ [P1 service] ──▶ [P5 query execution]
                                      gate → anchor(index lookup) →
                                      per-hop: adjacency read + intersect →
                                      var-length BFS: frontier expand (dedup) →
                                      changelog-tail scan → tail-merge → fetch → respond
                          [P6 reader replay]  manifest poll → WAL replay ──▶ replayed m/latest_seq
                          [P0 object store]   every GET/PUT underneath everything
```

- **P0 — instrumented `ObjectStore`** (Decision 3): the floor under every other number. If P2 or P5 is slow and P0 isn't, the time went to CPU/work-count, not S3 — that single bisection halves every investigation. On S3 the request *count* is both the bill and the latency budget, so P0 counts requests first, bytes second.
- **P1 — HTTP service** (RFC 0008): standard RED per route; detail deferred to RFC 0008, with the requirement that its labels reuse this RFC's outcome enums.
- **P2 — write fan-out** (RFC 0004): phase timers named for RFC 0004's own fan-out steps (encode node, encode out/in posting lists, index fan-out, `WriteBatch` commit, `m/latest_seq`).
- **P3 — build-loop tick** (RFC 0006): the producer whose lag becomes everyone else's changelog tail.
- **P4 — posting substrate** (RFC 0005): where RMW write amplification and supernode split problems live.
- **P5 — query execution** (RFC 0007): the read phases — gate, anchor/index lookup, per-hop adjacency read + intersect, variable-length BFS frontier expansion, changelog-tail scan, tail-merge, fetch — one timer each, one struct.
- **P6 — reader replay** (RFC 0001/0008): replication lag that becomes gate wait.

## 3. Metric inventory

Conventions: prefix `turbolay_`, snake case, base units in the name (`_seconds`, `_bytes`, `_total` for counters). Histogram bucket families: **`lat`** = {1ms, 2.5ms, 5ms, 10ms, 25ms, 50ms, 100ms, 250ms, 500ms, 1s, 2.5s, 5s, 10s} (spans "block-cache hit" to "gave up"); **`count`** = powers of 2 from 1 to 65536 (frontier and posting sizes reach the high end on supernodes); **`bytes`** = powers of 4 from 64 B to 4 MiB. `index` label = `"{index_id}:{kind}"`; `predicate` label = the schema predicate name (both bounded by the registry, readable on dashboards).

### 3.1 P0 — object store (ground truth: request count, latency, atomicity)

| Metric | Type / labels | Why — suspect it discriminates |
|---|---|---|
| `turbolay_objstore_requests_total` | counter `{op=get\|put\|delete\|list\|head, outcome=ok\|error\|retry}` | Request *count* is the S3 bill (GETs and PUTs are priced per request) and the latency budget. A cost regression, a cache-effectiveness collapse, or a traversal fanning out into N+1 GETs shows up here first. This is the metric the whole "scarce resource is requests, not bytes" thesis is built on. |
| `turbolay_objstore_request_duration_seconds` | histogram `lat` `{op}` | Is S3 itself slow today, or are we doing more of it? Compares directly against P2/P5 phase timers, and is the raw material for cold-vs-warm first-hop latency (§3.5). |
| `turbolay_objstore_bytes_total` | counter `{op, direction=tx\|rx}` | Compaction rewrite volume, WAL volume, posting read amplification — the secondary story (bytes are cheap on S3; requests are not). |
| `turbolay_objstore_cas_total` | counter `{outcome=ok\|conflict}` | Conditional-PUT / manifest CAS attempts (the fencing + atomicity primitive, D2). A nonzero `conflict` rate on the single writer means a zombie writer is being fenced — cross-references the invariant events (§3.7). |

Cost is a derived dashboard panel (`requests_total` × published per-request price), not a metric. SlateDB's own public stats surface, if/where exposed, is re-exported verbatim under a `slatedb_` prefix as best-effort passthrough; the wrapper above is the contract that works regardless of upstream — D1-safe, no fork.

### 3.2 P2 — write fan-out (RFC 0004)

| Metric | Type / labels | Why |
|---|---|---|
| `turbolay_write_requests_total` | counter `{op=upsert_node\|upsert_edge\|delete, outcome=ok\|oversize_node\|error}` | Throughput and the RFC 0014 trigger: a nonzero `oversize_node` rate is *the* datum for revisiting the node size cap vs building spill-to-raw-S3. |
| `turbolay_write_ack_duration_seconds` | histogram `lat` `{op}` | SLI §1. Baseline for the S3 Express WAL before/after. |
| `turbolay_write_phase_duration_seconds` | histogram `lat` `{phase=encode_node\|encode_out\|encode_in\|index_fanout\|batch_commit\|latest_seq}` | RFC 0004's fan-out steps verbatim. `batch_commit` (the durable PUT) should dominate; if `encode_node` grows with property-blob size it's codec evidence (§5); if `index_fanout` grows it means an edge touched many indexes (a heavily-indexed predicate). |
| `turbolay_write_edges_per_batch` | histogram `count` | Edges packed into one atomic batch — the fan-out width. High values amortize the PUT well; a workload writing one edge per batch is paying full S3 round-trip latency per edge (an advisory signal for client-side batching). |
| `turbolay_write_node_encoded_bytes` | histogram `bytes` | Distance-to-cap distribution (alert as p99 approaches the RFC 0004 cap); denominator for "is write latency just node size?" |
| `turbolay_write_inflight` | gauge | Pipelining depth under the single-writer ordering discipline; a stuck serialization point shows as inflight climbing while acks flatline. |
| `turbolay_latest_seq` | gauge | The anchor every lag metric subtracts from. Also: its rate *is* accepted-write throughput. |

### 3.3 P3 — build loop, watermarks, lifecycle (RFC 0006)

| Metric | Type / labels | Why |
|---|---|---|
| `turbolay_index_watermark` | gauge `{index}` | Raw material for RFC 0007's `W = min(watermark)`; graphed against `turbolay_latest_seq`. |
| `turbolay_index_watermark_lag_entries` | gauge `{index}` | = `latest_seq − wm`. Directly bounds the next query's changelog-tail size; compare against the tail bound for headroom. |
| `turbolay_index_watermark_lag_seconds` | gauge `{index}` | The human-meaningful lag. Mechanism below. |
| `turbolay_build_tick_duration_seconds` | histogram `lat` `{phase=registry\|scan\|fetch\|extract\|apply\|commit}` | RFC 0006's shared build-loop steps verbatim. The *only* way to answer "why is the builder behind": node fetch gone cold (S3), extract CPU (codec), apply RMW (posting-list shape / split), or commit (batch size / S3). |
| `turbolay_build_entries_applied_total` | counter `{index}` | Builder throughput; rate vs `turbolay_latest_seq` rate tells you whether lag is growing or draining. |
| `turbolay_build_node_fetch_duration_seconds` | histogram `lat` | Bimodality here = block-cache-hit vs S3-read split without needing SlateDB internals — the builder-side twin of cold-first-hop. |
| `turbolay_backfill_progress_uid` / `turbolay_backfill_nodes_scanned_total` | gauge / counter `{index}` | ETA for `backfilling → live` (RFC 0006 lifecycle); a flat progress gauge is a stuck backfill. |
| `turbolay_index_state` | gauge `{index, state}` (0/1) | Lifecycle visibility (`creating→backfilling→live→dropping`); transitions additionally emit structured events (§6). |

**Lag-in-seconds mechanism (no format change).** The changelog record deliberately carries no wall-clock timestamp (RFC 0004), and this RFC does not add one. The builder runs in the writer process (RFC 0006), so the writer keeps a bounded in-memory ring of `(seq, Instant)` for recent writes; `lag_seconds = now − ring[wm]` while `wm` is inside the ring, else reported as `> ring_window` (saturated). The ring covers minutes of writes for kilobytes of memory; healthy lag is "seconds, one batch," so saturation itself is a red flag. Reader-side lag-in-seconds cannot use this (different process) — see §3.6.

### 3.4 P4 — posting substrate (RFC 0005)

| Metric | Type / labels | Why |
|---|---|---|
| `turbolay_posting_splits_total` | counter `{predicate}` | Supernode 512-KiB bin-splits (RFC 0005). Split rate ≈ supernode formation health; a predicate that suddenly starts splitting is growing a hub node. |
| `turbolay_posting_parts` | histogram `count` `{predicate}`, sampled at write | **Part count per (src,predicate) — the supernode fan-out signal.** A posting list in many parts means a hub node whose adjacency read is now a multi-GET; this is the metric that says "traversal through this predicate is expensive because the node is huge," and the direct before/after for CSR (RFC 0009). |
| `turbolay_posting_rmw_total` | counter `{predicate}` | RMW applications per tick; numerator of write amplification (split/rollup are single-writer RMW, not merge — D11). |
| `turbolay_posting_bytes_written_total` | counter `{predicate}` | With `turbolay_uids_added_total`: **write amplification = bytes written / uid added** — `plan.md`'s first-listed risk, now a chartable ratio. Baseline for block-threshold and bitpacked-frame tuning (RFC 0010). |
| `turbolay_uids_added_total` | counter `{predicate}` | Edges materialized into posting lists; denominator of write amp and of per-predicate skew analysis via the sampled tick log (§6). |
| `turbolay_posting_block_uids` / `turbolay_posting_block_bytes` | histogram `count`/`bytes` `{predicate}`, sampled | Block-shape distribution (256-uid UidPack blocks). The direct before/after for bitpacked frames (bytes/block drops, uids/block unchanged — RFC 0010). |
| `turbolay_posting_part_cache_misses_total` | counter `{predicate}` | Cold part-locates. A high steady-state rate means the working set of supernodes exceeds cache — a locality signal feeding RFC 0009. |

### 3.5 P5 — query execution (RFC 0007)

| Metric | Type / labels | Why |
|---|---|---|
| `turbolay_query_requests_total` | counter `{consistency, outcome=ok\|reader_behind\|index_behind\|unindexed_property\|malformed_cypher\|unsupported_cypher\|oversize_node\|error}` | The error taxonomy is locked (RFC 0007/0008) — this counter is it, verbatim. |
| `turbolay_query_duration_seconds` | histogram `lat` `{consistency,max_hops}` | SLI §1. |
| `turbolay_query_phase_duration_seconds` | histogram `lat` `{phase=gate\|anchor\|hop\|frontier_expand\|tail_scan\|tail_merge\|fetch}` | RFC 0007's read phases verbatim. The single most important chart in the system: p99 decomposes into one guilty phase in one look. `hop` and `frontier_expand` are the traversal phases; the rest mirror the index+tail plan. |
| `turbolay_query_gate_wait_seconds` | histogram `lat` `{consistency}` | Freshness SLI; separates "reader was caught up" (≈0) from "waited for replay." |
| `turbolay_query_first_hop_duration_seconds` | histogram `lat` `{warmth=cold\|warm}` | **Cold vs warm first-hop latency** (SLI §1). Derived from P0 request latency at the anchor + first adjacency read: `cold` = a first-hop that paid ≥1 S3 GET, `warm` = served from block cache. The primary evidence that cold first-hop is the enemy, and the target metric for CSR/locality (RFC 0009). |
| `turbolay_query_frontier_size` | histogram `count` `{hop_index}` | **Per-hop frontier sizes.** How many UIDs are live at each BFS depth. This is the fan-out fingerprint of a traversal: a frontier that explodes from hop 1→2→3 tells you *where* a `*1..3` query got expensive, and whether filter push-down (RFC 0007) is actually shrinking frontiers before the next expansion. `hop_index` is a small bounded enum (0..max_hops cap). |
| `turbolay_query_neighbor_reads_total` | counter `{batched=true\|false}` | **N+1 neighbor fan-out — the dominant traversal cost (RFC 0007 names it explicitly).** Counts adjacency reads issued during frontier expansion, split by whether the planner batched them or read one node at a time. `batched=false` climbing is the literal N+1 problem happening; the ratio of neighbor-reads to frontier-size is how well batching is working, and this is *the* number CSR (RFC 0009) must move. |
| `turbolay_query_adjacency_parts_read` | histogram `count` | Posting-list parts touched per hop — supernode traversal cost (a hub node's adjacency spans many parts, §3.4). |
| `turbolay_query_tail_entries` | histogram `count` | **Changelog-tail entries scanned per query.** Per-query tail size vs the RFC 0007 bound; its p99 is the early warning for `index_behind`. |
| `turbolay_query_postings_decoded` | histogram `count` | Intersection/merge work units (sorted-UID `algo`, RFC 0005). Baseline for bitpacked frames (decode cost) and block-max skipping (postings *not* decoded once RFC 0010 lands, at constant candidates). |
| `turbolay_query_intersect_ops_total` | counter `{op=intersect\|union\|difference}` | Which set operation dominates: `AND`=intersect, `OR`=union, `NOT`=difference (Dgraph's `algo`, ported). A standalone `NOT` doing a live-universe difference is visible here rather than semantically special-cased. |
| `turbolay_deleted_bitmap_cardinality` / `turbolay_deleted_bitmap_bytes` | gauge `{predicate}` | **Deleted-UID bitmap cardinality per predicate.** RFC 0012's stated trigger ("deleted-bitmap cardinality growth"), plus difference-phase cost denominator. Grows monotonically in v0 (no vacuum until RFC 0012). |
| `turbolay_query_nodes_fetched` / `turbolay_query_rows_returned` | histogram `count` | Fetch-phase work and the result-size SLI (§1). `nodes_fetched ≫ rows_returned` means a big frontier was materialized to produce few rows — a projection/selectivity signal. |
| `turbolay_query_bruteforce_total` | counter | Guardrailed escape hatch usage — the opt-in unindexed-predicate scan (RFC 0006). Should be rare and deliberate; a steady rate means a missing index in some client. |

### 3.6 P6 — reader fleet (RFC 0001 / 0008)

| Metric | Type / labels | Why |
|---|---|---|
| `turbolay_reader_replayed_seq` | gauge | Per-reader replication position; dashboards join against the writer's `turbolay_latest_seq` for **reader-behind rate** in entries (cross-process, so the subtraction happens at query time in the dashboard). |
| `turbolay_reader_manifest_polls_total` / `turbolay_reader_wal_ssts_replayed_total` | counter | Replay liveness: polls happening but no SSTs replayed = quiet writer; neither = stuck reader. |
| `turbolay_reader_replay_duration_seconds` | histogram `lat` | Replay cost per poll cycle; the input side of gate-wait spikes. |
| `turbolay_block_cache_hit_ratio` | gauge (derived) | **Block-cache hit rate** — the direct driver of cold-vs-warm first-hop. Derived from cache hits/misses exposed by the storage layer (or, failing upstream exposure, inferred from the P0 `warm` split). A dropping hit rate predicts cold-first-hop latency rising. |

**Reader lag-in-seconds (Phase 2, optional durable addition).** A reader knows *what* seq it has replayed but not *when* that seq was written. Proposal: the writer periodically (~1s) puts `m/obs_heartbeat → [tag] ++ U64(seq) ++ U64(unix_millis)` — an ordinary meta key, plain put, outside every locked value format. Readers report `now − replayed_heartbeat.time` as replication lag in seconds (caveat: cross-node clock skew; acceptable for an ops signal). **This is a new `m/` key and must be registered in RFC 0003's `Meta` grammar before implementation** — flagged here rather than silently added; everything else in this RFC works without it (entries-based reader-behind rate needs no new keys).

### 3.7 Invariant counters and lifecycle events (should-be-zero / audit trail)

`turbolay_invariant_violations_total{kind}` — alert at ≥ 1, each `kind` converting an RFC's "structurally impossible" claim into a monitored fact. These are the cheapest guard on the contracts D6/D10/D11 make the whole model depend on:

| `kind` | The claim it monitors |
|---|---|
| `projection_asymmetry` | An `EdgeOut` with no matching `EdgeIn` (or vice versa) observed on a consistency check — RFC 0004's "every edge written twice in one atomic batch" (D10) contract. The single most important graph invariant: reverse traversal correctness depends on it. |
| `orphaned_index_entry` | An index posting-list entry pointing at a UID whose node/edge no longer satisfies the indexed predicate (outside the expected deleted-bitmap window) — RFC 0006 index-consistency contract. |
| `xid_uid_miss` | A `Xid` lookup resolved to a UID with no corresponding node record, or a node whose `xid` doesn't round-trip — RFC 0004's `xid → uid` mapping-recovery contract (D5). |
| `changelog_gap` | A gap in the `Log[seq]` sequence (a missing seq between watermark and latest) — RFC 0004's monotonic single-writer seq contract (D4). |
| `watermark_overshoot` | `m/wm/{id} > m/latest_seq` observed — RFC 0006's "never an overestimate" contract. |
| `posting_order_violation` | A posting stream yielded a non-strictly-ascending UID (debug-assert in tests; sampled check in release) — RFC 0005's sorted-UID stream contract that intersection relies on. |
| `merge_rejected` | The merge operator fired its reject arm (RFC 0003 dispatch table) — some path issued a merge to a non-merge prefix (D11). |
| `recovery_probe_failed` | RFC 0004's defensive recovery point-read failed on writer open (the writer refuses to start; the counter makes it visible to monitoring). |
| `decode_error` | Any versioned-envelope or layout decode failure (node / posting / changelog / index) — corruption or a format bug, never workload-dependent. |

Structured **events** (log records with fields, not metrics): index lifecycle transitions (`creating→backfilling→live→dropping`, with `index`, `snapshot_seq`), writer open/recovery (recovered `next_seq`/`next_uid`), zombie-fencing trips (join with `objstore_cas_total{conflict}`), `index_behind`/`reader_behind` occurrences (with the seq numbers from the error body), supernode split events (`predicate`, `src_uid`, new part count), and the per-tick top-N predicates by UIDs added (§6 — the cardinality-safe answer to "which predicate is hot").

### 3.8 P1 — HTTP service (RFC 0008)

Standard RED (rate/errors/duration) per route and status. Owned by RFC 0008; the binding requirement from this RFC: route-level outcome labels must reuse §3.5's taxonomy values (an `index_behind` is an `index_behind` at every layer), and the `/metrics` exporter lives on the internal/admin plane only (D9 posture — telemetry is not a public data-plane surface).

## 4. Usual-suspects ledger

Symptom → suspects → the metrics that convict or exonerate, written before the first incident. This is the operational payoff of §3: every row resolves with data already being collected.

| Symptom | Usual suspects, in likelihood order | Discriminating metrics |
|---|---|---|
| **Write acks slow** | (1) S3 PUT latency / `flush_interval`; (2) heavy index fan-out on the edge; (3) big node blob; (4) codec cost | `write_phase{batch_commit}` vs `objstore{put}` (S3 itself?) → `write_phase{index_fanout}` (many indexes?) → `write_node_encoded_bytes` (size-correlated?) → `write_phase{encode_node}` |
| **Queries slow (blended)** | first: *which phase?* | `query_phase_duration_seconds` — then descend per phase, rows below |
| — gate slow | reader replay lagging; client tokens hot off the writer | `gate_wait{session}`, `reader_replayed_seq` vs `latest_seq`, `reader_replay_duration`, manifest poll cadence |
| — anchor slow | cold anchor index part; low-selectivity anchor chosen | `first_hop_duration{cold}` → `objstore{get}` latency → `block_cache_hit_ratio` |
| — hop / frontier slow | **N+1 fan-out** (dominant); supernode adjacency (many parts); cold adjacency GETs | `query_neighbor_reads_total{batched=false}` rate → `query_frontier_size{hop_index}` (where it explodes) → `query_adjacency_parts_read` (supernode?) → `objstore{get}` latency |
| — tail slow | builder lag (big tail); tail nodes large | `query_tail_entries` vs `watermark_lag_entries` → `build_tick_duration{phase}` for *why* the builder lags → `write_node_encoded_bytes` |
| — tail-merge / difference slow | deleted bitmap large | `deleted_bitmap_cardinality{predicate}` / `_bytes`, `query_intersect_ops_total{difference}` |
| — fetch slow | many nodes materialized; large nodes; S3-cold node reads | `query_nodes_fetched`, `query_rows_returned`, `objstore{get}` |
| **First hop slow only when cold** | block cache too small / working set too big; no locality (uid scatter) | `first_hop_duration{cold}` vs `{warm}` gap → `block_cache_hit_ratio` → `posting_part_cache_misses_total` — **this is the RFC 0009 trigger** |
| **`reader_behind` spiking** | reader restarted (cold replay); poll interval too coarse; writer burst outpacing replay | `reader_wal_ssts_replayed_total` rate, `reader_replay_duration`, `latest_seq` rate |
| **`index_behind` spiking** | builder stuck or slow — which tick phase?; one hot predicate concentrating a batch; backfill of a new index stealing the loop's I/O | `watermark_lag_entries` trend → `build_tick_duration{phase}` → per-tick top-N-predicates event → `backfill_nodes_scanned_total` active? |
| **S3 cost / request-rate rising** | cache effectiveness collapsed; traversal N+1-ing into GETs; builder RMW churn; a client polling `strict` | `objstore_requests_total{op}` split by rate change → `query_neighbor_reads_total{batched=false}` → `posting_bytes_written_total` → `query_requests_total{strict}` |
| **Storage growing** | dead UIDs + bitmap (no vacuum in v0 — expected); changelog `l/` grows forever in v0 (no GC until RFC 0012); posting churn | `deleted_bitmap_cardinality` vs UIDs allocated, `latest_seq` (≙ changelog entry count), `objstore_bytes_total{put}` |
| **Results look wrong** | (should be never) | `invariant_violations_total` — any nonzero kind localizes the class of bug immediately; `projection_asymmetry` in particular points straight at the write fan-out |

## 5. Optimization before/after matrix (binding on RFC 0009+) — and the real-S3 rule

Every deferred item in RFC 0000's correct-first ledger, mapped to the **primary metric** that must improve and the **guardrail** that must not regress.

> **HARD RULE (D12): benchmark on real S3, never LocalStack-only.** Every baseline in this matrix and every before/after comparison **must be captured against real S3** (S3 Standard for bulk, S3 Express for WAL where configured), not LocalStack, MinIO, or an in-memory store. **Rationale:** the namidb prior attempt benchmarked on LocalStack and it **hid a ~10× cold-read regression** — LocalStack serves objects from local disk/memory at single-digit-millisecond latency and does not reproduce S3's cold-object first-byte latency. For an S3-backed graph, **cold first-hop S3 latency is the real enemy** (SLI §1, §4): a traversal's cost is dominated by cold adjacency and node GETs, exactly the thing LocalStack makes look free. An optimization "win" measured on LocalStack is therefore unfalsifiable and may be a mirage. In-memory / LocalStack harnesses remain valid for *correctness* tests and for the overhead smoke check (§9); they are **forbidden as the basis for any performance baseline or before/after claim.**

Rule, restated per-row: *the metric must exist and have a recorded baseline **on real S3** before the optimization merges, and the metric definition must not change across it* — otherwise the before/after claim is unfalsifiable. **RFC 0009 does not start until Phase 3 (§8) is live and real-S3 baselines are captured; this RFC's benchmark-grade phase is a hard gate on RFC 0009+.**

| Deferred optimization (owner) | Primary metric(s) — must move (on real S3) | Guardrail — must not regress |
|---|---|---|
| CSR adjacency + WCOJ/leapfrog joins (0009) | `query_first_hop_duration{cold}`; `query_neighbor_reads_total{batched=false}`; `query_phase{hop,frontier_expand}`; `objstore_requests_total{get}` per traversal | traversal correctness (FalkorDB shadow-test, D12); write-path cost (CSR build must not blow up `build_tick_duration` / `write_phase`) |
| Bitpacked posting frames (0010) | `query_postings_decoded` per unit time in `query_phase{hop}`; `posting_block_bytes` (smaller blocks); index bytes on S3 | candidate sets bit-identical (BTreeSet oracle, RFC 0005); `build_tick{apply}` / `write_phase` time (encode cost moved, not exploded) |
| Block-max WAND / MAXSCORE (0010) | `query_postings_decoded` at constant candidates (skipping working); `query_duration` p99 for selective traversals | result correctness vs exact-intersect oracle |
| Vacuum / dead-UID purge (0012) | `deleted_bitmap_cardinality{predicate}`; difference-phase cost (`query_intersect_ops_total{difference}` × bitmap size) | query correctness through vacuum; `build_tick_duration` (vacuum stays off the hot loop); `objstore` PUT volume during vacuum bounded |
| Locality-aware partitioning (measured; noted here) | `block_cache_hit_ratio`; `query_first_hop_duration{cold}` gap; `posting_part_cache_misses_total` | write-path locality cost; compaction churn (`objstore_bytes_total{put}`) |
| rkyv vs CBOR node codec (0004 spike, fast-follow) | `build_tick{extract}`; `query_phase{fetch}` per node; `write_phase{encode_node}` | round-trip correctness (property tests); `write_node_encoded_bytes` (format size delta) |
| S3 Express WAL (config note) | `write_ack_duration_seconds` p50/p99 | `objstore_requests_total` cost delta (Express pricing); durability semantics unchanged |
| Oversized-node spill to raw S3 (0014) | trigger: `write_requests_total{outcome=oversize_node}` rate | — |
| Standalone/distributed compaction (config flip) | `build_tick_duration` / `write_ack` interference during compaction; `objstore` op contention | compaction throughput (passthrough / `objstore_bytes_total`) |
| Full openCypher / aggregation (0013) | trigger: product need; consumes the 0007 IR (frontend change, not perf) | `query_phase{plan}` — a heavier frontend must not dominate the small-query path |

## 6. Cardinality, naming, and the hot-supernode problem

- **Legal labels**: `namespace`; `predicate` (schema/registry-bounded); `index` (`"{id}:{kind}"`, registry-bounded); closed enums (`op`, `phase`, `outcome`, `consistency`, `state`, `kind`, `warmth`, `hop_index` capped at max_hops, `batched`). Budget: the full export for one namespace stays under ~2,000 series (test-enforced, §9).
- **Never labels**: UIDs, xids, encoded tokens, seq values, property values, per-node degree, raw Cypher text, `src_uid`/`dst_uid`. Every one of these is unbounded and would eventually take the metrics pipeline down — the classic self-inflicted observability outage.
- **Hot-supernode / hot-predicate visibility without label explosion**: per tick, the builder emits one structured log event with the top-N (default 8) predicates by UIDs added and the largest supernode (part count) seen; supernode split events (§3.7) carry the `src_uid` in the *log*, not a label. Aggregation over these is a log-search problem, deliberately not a metrics problem.
- **Slow-query log**: any query whose total duration exceeds `slow_query_threshold` (default 500ms) — or which errored `index_behind`/`reader_behind` — logs its complete `QueryStats` struct: all phase timings, per-hop frontier sizes, N+1 neighbor-read counts, adjacency parts read, changelog-tail entries scanned, postings decoded, nodes fetched, rows returned, consistency mode, `max_hops`, S3 GET count, and a *structural* summary of the pattern (labels, predicates, operators, registry paths only — never literal property values or xids, which are user data). This is the bridge from "p99 moved" to "here are the actual offending traversals."
- **Per-query debug stats**: with `"debug": true` on the query request (RFC 0007/0008 grammar extension), the response carries the same `QueryStats` under a `"debug"` key — phases, posting lists / adjacency parts read, per-hop frontier sizes, tail entries, and S3 GETs. Zero-infrastructure profiling for a developer using the public API. Grammar note: this extends RFC 0007's request object with one optional key (RFC 0007/0008 to ratify; rejected-unknown-key rules otherwise unchanged).

## 7. Failure and safety properties of the instrumentation itself

- **Telemetry is loseable by design.** No metric or event participates in any `WriteBatch`, watermark, or recovery path; a metrics failure (exporter down, buffer full) degrades to silence, never to a write/read error. This is also why metrics do **not** live in the merge-counter keyspace (see Alternatives).
- **Single emission point** (Decision 4): stats structs are recorded at operation completion, on success *and* error paths, exactly once — phase timers that never completed record what they had (a query that errored `unindexed_property` at anchor still emits its partial struct). No lock is held across a metric write; recording is wait-free counter/histogram math.
- **Bounded memory**: the seq→time ring (§3.3), the top-N predicate accumulator, the per-hop frontier-size accumulator (bounded by max_hops), and the slow-query serialization are all fixed-size; instrumentation cannot grow with workload or with frontier size.
- **Overhead budget**: ≤ ~1% end-to-end on the in-memory harness (loose smoke check, §9), which at S3-dominated latencies is far below noise — the point of checking is catching an accidental hot-loop histogram (e.g. a per-UID histogram inside frontier expansion), not micro-tuning.

## 8. Phasing — most essential first

| Phase | Lands with | Contents | Why first |
|---|---|---|---|
| **0 — spine** | M0/M1 code (now) | `metrics` facade wiring; **P0 object-store wrapper**; **write-path fan-out timers** (§3.2); `turbolay_latest_seq`; error/outcome counters; **invariant counters** (§3.7); structured-event conventions; naming/cardinality rules | The object-store wrapper and write fan-out are the ground truth everything else is interpreted against, and they instrument code being written *right now*; retrofitting probes is 10× the cost of landing them with the code. Invariant counters guard the D10 bidirectional-storage contract from the first edge written. |
| **1 — the matrix** | M2 (exit criterion) | Watermark + lag entries/seconds; build-tick phase breakdown; posting split counts + part counts; write-amp counters; block-shape sampling; **query phase timers**; **per-hop frontier sizes**; **N+1 neighbor-fan-out counters**; changelog-tail entries scanned; deleted-bitmap cardinality per predicate; adjacency parts read; intersect/union/difference counters | This is the graph metric matrix in full: *latest seq ✓, per-index watermark lag ✓, tail entries scanned ✓, deleted-bitmap cardinality ✓, posting split/part counts ✓, per-hop frontier sizes ✓, N+1 fan-out ✓, query phase timings ✓, reader/index-behind rate ✓ (Phase 0)*. M2's end-to-end consistency test should assert against these metrics (e.g. prove a freshly-written edge arrived via the tail path by reading `query_tail_entries`; prove reverse traversal works by asserting `projection_asymmetry == 0`), making the instrumentation itself tested. |
| **2 — fleet & service** | M3 | HTTP RED (with RFC 0008); per-reader replay metrics + reader-behind rate; block-cache hit ratio; optional `m/obs_heartbeat` (after RFC 0003 registration); **slow-query log**; **`"debug": true` response stats**; `/metrics` on the admin plane; starter dashboards + alert pack (below) | Needs the service and reader fleet to exist (RFC 0008). The FalkorDB shadow-test (D12) runs here, and the slow-query log is what explains any set-diff. |
| **3 — benchmark grade** | after M3, **gates RFC 0009+** | Cold-vs-warm first-hop split; N+1-per-frontier accounting; postings-decoded throughput; **the real-S3 baseline capture procedure** for §5's matrix (recorded numbers checked into the benchmark harness, captured against real S3 per the §5 hard rule) | RFC 0009 "runs on data, not intuition" — Phase 3 *is* that data, and it must be real-S3 data. **No real-S3 baseline, no optimization PR.** |

**Alert starter pack (Phase 2)**: `invariant_violations_total > 0` (page — especially `projection_asymmetry`); `watermark_lag_seconds > 30s` sustained (warn — tail pressure building); `query_tail_entries` p99 > 0.5 × tail bound (warn — `index_behind` imminent); `query_neighbor_reads_total{batched=false}` rate climbing (warn — N+1 regressing); `write_requests_total{oversize_node}` rate > 0 (ticket — cap pressure, RFC 0014 trigger); `reader_replayed_seq` flat while `latest_seq` moves (page — stuck reader); `objstore_requests_total{outcome=error}` rate (warn); `block_cache_hit_ratio` dropping with `first_hop_duration{cold}` rising (ticket — locality/CSR trigger, RFC 0009); `deleted_bitmap_cardinality` growth (ticket — vacuum trigger, RFC 0012).

## 9. Tests required

1. **Phase-accounting completeness.** For a scripted end-to-end scenario on the in-memory harness (write a subgraph → lagging index → session-token traversal served via the changelog tail), assert: every §3.5 phase histogram has samples; phase durations sum to ≈ total query duration (tolerance for gaps); `query_tail_entries` equals the constructed tail size exactly; `query_frontier_size{hop_index}` matches the known per-hop frontier of the fixture graph; the neighbor-read count matches the expected fan-out.
2. **Suspect discrimination.** Construct each usual-suspect scenario the harness can express (artificial watermark hold → `watermark_lag_entries` and `index_behind`; standalone `NOT` → `intersect_ops_total{difference}`; a supernode → `posting_parts` high and `adjacency_parts_read` high; an unbatched traversal → `neighbor_reads_total{batched=false}` high; delete-heavy workload → `deleted_bitmap_cardinality > 0`) and assert the designated discriminating metric, and only it, moves.
3. **Invariant counters stay zero** across the entire existing test suite (RFC 0003–0007 test sets run with metrics on); separately, inject each violation kind via the harness (write an `EdgeOut` without its `EdgeIn` → `projection_asymmetry`; delete a node under a live index entry → `orphaned_index_entry`; skip a seq → `changelog_gap`) and assert the counter fires with the right `kind`.
4. **Cardinality budget.** After the full suite, dump the exporter's series list: assert count under budget and assert **no label value contains user data** (scan for seeded UIDs / xids / property values / raw Cypher in label values — this is the test that keeps §6 honest forever).
5. **Emission-on-error.** Force each taxonomy error (`reader_behind`, `index_behind`, `unindexed_property`, `malformed_cypher`, `unsupported_cypher`, `oversize_node`) and assert exactly one `query_requests_total`/`write_requests_total` increment with the right outcome and a stats emission with partial phase timings — no double count, no silent drop.
6. **Overhead smoke.** In-memory-harness throughput with recorder installed vs no-op recorder within the loose ≤ ~1% budget (a regression tripwire, not a benchmark — real perf numbers come only from the real-S3 procedure of §5/Phase 3).

## Alternatives considered

### OpenTelemetry (traces + metrics + logs) as the primary framework

Pros: one standard, distributed-tracing-ready, vendor-neutral export. Cons: turbolay v0 is one writer process and a handful of readers per namespace (D2/D9) — there is no cross-service trace to stitch; OTel's SDK surface and pipeline complexity are large relative to a POC's needs; Prometheus pull + structured logs cover every §1–§5 requirement. Rejected as primary; the `metrics` facade keeps recording sites exporter-agnostic, so adding an OTLP exporter later (e.g. when the reader fleet grows real topology) is a wiring change, not an instrumentation rewrite.

### Always-on per-hop tracing spans

Pros: maximal per-traversal detail. Cons: span machinery per frontier-expansion step is real overhead and near-duplicate information — and a variable-length `*1..k` traversal over a supernode would emit thousands of spans per query. The `QueryStats` struct + slow-query log + `debug: true` response give per-traversal forensics (including per-hop frontier sizes and N+1 counts) precisely when wanted, at zero cost when not. Rejected; `tracing` is used for structured *events* (§3.7, §6), not per-hop span trees.

### Storing metrics in the database itself (e.g., under merge-counter keys)

Superficially attractive — the merge-counter machinery (D11) exists and survives restarts. Rejected outright: it puts telemetry writes on the durability hot path (every observation becomes S3 bytes and compaction work — the instrument perturbs the measurement, and *requests are the scarce resource we are trying to measure*), couples ops data to the keyspace/GC lifecycle, and would tempt widening RFC 0003's closed merge dispatch table. Telemetry must be loseable (§7); process-local registries scraped over HTTP are exactly that. The one narrow exception is the Phase 2 `m/obs_heartbeat` key, which exists *because* readers can only learn writer wall-clock through the replicated keyspace — one key, plain put, explicitly registered.

### Wall-clock timestamps in changelog records for lag-in-seconds

Pros: exact per-entry age, works cross-process. Cons: changes RFC 0004's locked changelog record layout for an ops nicety; embeds nondeterministic wall-clock into replayed durable state; the in-process ring (§3.3) answers the builder-lag question and the heartbeat key (§3.6) answers the reader-lag question without touching any locked format. Rejected.

### Per-UID / per-supernode metric labels for hot-node analysis

Rejected per §6 — unbounded cardinality is how metrics pipelines die, and UIDs are the highest-cardinality thing in the system. Sampled top-N structured events and supernode split events give the same investigative power with fixed cost, at the price of log-search instead of instant dashboards; acceptable, since hot-supernode analysis is an investigation activity, not a standing dashboard.

### Benchmarking on LocalStack / MinIO / in-memory for convenience

Rejected as a hard rule (§5, D12). LocalStack does not reproduce S3 cold-object first-byte latency — the namidb prior attempt did this and it hid a ~10× cold-read regression. For an S3-backed graph, cold first-hop latency is the dominant cost and the entire point of the optimization ledger; a baseline that hides it is worse than no baseline because it manufactures false confidence. Local emulators keep their role for correctness and overhead-smoke tests only; every performance baseline and before/after claim is captured on real S3.

## Final contract

- Every phase named by RFCs 0004–0007 has a timer named after it; the read-path `turbolay_query_phase_duration_seconds` (gate / anchor / hop / frontier_expand / tail_scan / tail_merge / fetch) and the build-loop `turbolay_build_tick_duration_seconds` (registry / scan / fetch / extract / apply / commit) histograms are the two canonical decomposition charts, and the instrumented `ObjectStore` wrapper is the ground-truth layer beneath both — D1-safe, no SlateDB fork.
- On S3 the scarce resources are **requests, latency, and atomicity, not bytes**: the object-store wrapper counts requests first, and **N+1 neighbor fan-out** (`turbolay_query_neighbor_reads_total`) plus **per-hop frontier sizes** (`turbolay_query_frontier_size`) are first-class metrics because the neighbor fan-out is the dominant traversal cost.
- The user-facing SLIs (§1) are defined on the public JSON-write / openCypher-read surface: write ack latency, query latency by consistency mode and hop depth, gate wait, retryable/hard error rates, result size, index freshness, and **cold-vs-warm first-hop latency**. Internal metrics exist to explain these.
- The graph metric matrix is fully covered and lands as an M2 exit criterion: latest seq, per-index watermark lag (entries and seconds), changelog-tail entries scanned, deleted-bitmap cardinality per predicate, posting split + part counts per supernode, per-hop frontier sizes, N+1 fan-out, query phase timings, reader-behind rate, cold-vs-warm first-hop latency, and block-cache hit rate.
- The usual-suspects ledger (§4) and the optimization before/after matrix (§5) are binding documentation: each deferred optimization's primary and guardrail metrics must exist, **with baselines recorded on real S3**, before that optimization merges. **RFC 0009+ is gated on Phase 3 being live; benchmarking on LocalStack-only is forbidden** (D12) — the namidb precedent (a hidden ~10× cold-read regression) is the reason.
- Invariant counters convert the RFC series' "structurally impossible" claims into monitored, should-be-zero facts — out/in projection asymmetry, orphaned index entries, xid→uid misses, changelog gaps, watermark overshoot, posting-order violations — alerting at the first occurrence.
- Cardinality rules are hard: `namespace`, registry-bounded `predicate`/`index`, and enum labels only; UIDs, xids, tokens, and property values never appear in a label (test-enforced). Hot-supernode/hot-predicate analysis flows through sampled structured events, the slow-query log, and the `debug: true` per-query stats object.
- Telemetry is loseable, wait-free at recording sites, bounded in memory, off every durability path, and never stored in the database — with the single, explicitly-registered exception of the optional Phase 2 `m/obs_heartbeat` key, which requires RFC 0003 registration before it exists.
- No locked format, key layout, or contract from RFCs 0001–0008 is modified by this RFC; the `"debug": true` request key is a grammar extension for RFC 0007/0008 to ratify.
