---
title: SlateDB reader sync and key-level refresh strategy
date: 2026-07-09
status: captured
area: distributed-readers
tags:
  - discord
  - slatedb
  - dbreader
  - object-store
  - cache-invalidation
---

# SlateDB reader sync and key-level refresh strategy

## Scenario

We expect a distributed Turbolay deployment to have one SlateDB-backed writer
instance and one or more read-only instances on other nodes. The writer
periodically flushes updates for a keyspace on the order of 100K keys to the
object store. The readers need to observe those updates without looping over
the full keyspace on every refresh cycle.

The concrete concern from Discord was:

- How do reader SlateDB instances learn about updates by default?
- If keys are constantly changing, should readers manually scan ranges from
  object storage?
- Should the application signal readers through pub/sub so they refresh
  particular keys?
- Is looping over all keys on the reader side the wrong shape?

## SlateDB behavior to assume

SlateDB readers do not provide a built-in per-key invalidation stream. A
`DbReader` refreshes against the source database at a configured interval, so
the default unit of freshness is database/manifest visibility rather than
individual key notifications.

The closest currently available lower-level hook is DB metadata subscription:
`DbMetadataOps::subscribe`, implemented by `DbReader`, can notify when the
current manifest changes. A reader can keep the previous manifest, compare it
with the new manifest, identify added or removed SSTs, and then inspect the
newly added SSTs to derive the set of keys that may need refresh. That is
possible, but it is non-trivial and still operates at the storage-file layer,
not at Turbolay's graph object layer.

## Implication for Turbolay

For distributed readers, full keyspace scans should be treated as a fallback or
repair operation, not the normal coherence path. Scanning all keys to discover
which keys changed wastes object-store requests, defeats locality, and becomes
more expensive as the graph cache gets warmer and the update rate rises.

Turbolay should make graph-layer freshness explicit:

- Readers should track a durable graph watermark, such as `latest_seq`, per
  cell or shard.
- Writers should keep enough change-log information for readers to tail changes
  from their last applied watermark.
- Reader caches should invalidate or refresh the graph objects named by the
  change log: adjacency postings, reverse postings, node records, edge
  properties, index entries, matrix artifacts, and supernode artifacts as
  applicable.
- Manifest subscription can be used to wake a reader quickly, but the manifest
  itself should not be the only semantic source of truth for graph changes.

This matches the broader reader direction already noted in
`docs/impl/2026-07-07-query-engine-walkthrough-main.md`: a future `GraphReader`
should be backed by an unfenced, manifest-polling `DbReader`, with
snapshot-consistent reads, schema refresh, batched reads, and a read-side cache.

## Recommended strategy

Use a two-layer sync protocol:

1. Durable pull path: each reader periodically checks the writer-visible graph
   watermark and tails the Turbolay change log from its last applied sequence.
   This path must be sufficient on its own after reader restart, missed
   notifications, or pub/sub loss.
2. Best-effort wakeup path: the writer publishes a compact notification when it
   commits. The notification should contain at least `{cell, commit_seq}` and
   may include touched key ranges or graph object classes. Readers use it only
   to shorten staleness, then still reconcile through the durable pull path.

Avoid per-key pub/sub as the primary protocol for high-churn workloads. It can
be useful for low-rate hot keys, but at high update rates it is better to batch
notifications by cell, predicate, range, artifact family, or sequence window.

When exact key-level refresh is needed below the graph layer, manifest diffing
is the current SlateDB-native option: subscribe to manifest changes, diff old
and new manifests, identify newly added SSTs, enumerate keys in those SSTs, and
refresh only those keys. That should be considered a specialized optimization
because it couples reader invalidation to LSM layout details.

## Open work

- Design the `GraphReader` sync loop around `DbReader` refresh/subscription and
  graph watermarks.
- Define the minimum change-log record needed for cache invalidation and index
  freshness.
- Decide whether reader notifications are carried over the existing control
  plane, a pub/sub system, or both.
- Add bounded reader caches with per-object invalidation instead of wholesale
  cache clears or all-key rescans.
- Keep full scans as a reconciliation/repair path with metrics, not as the
  steady-state update-discovery mechanism.
