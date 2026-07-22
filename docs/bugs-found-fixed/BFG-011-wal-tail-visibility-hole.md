---
id: BFG-011
title: WAL-file boundary hole in the compiled base plus WAL-tail merge
status: open
severity: P1
classification: unproven-suspect
introduced_or_first_bad_commit: pending-bisect
fix_commit: none
affected_range: present at 36a38a6 (Turbolay-V3.5)
model:
  intended: none-yet (candidate: M2 snapshot read family)
  fault: none-yet
current_verified_commit: 36a38a6
date_opened: 2026-07-22
date_verified: null
tags: [bugs, wal, graph-index, overlay, freshness, unproven]
---

# BFG-011: WAL-file boundary hole in the base + WAL-tail merge

## Status

**Not reproduced, and not cleared.** Two reproduction attempts both pass. The
tests are committed and `#[ignore]`-marked so they can be re-pointed at a
fault-injecting harness rather than rewritten. This record exists so the
suspect is not silently dropped on the strength of two negative results.

## The suspected hole

A compiled traversal answers from a base CSC at `generation.base_sequence`
plus a WAL-tail overlay covering `(base_sequence, read_epoch]`. Two lines
decide whether that overlay is complete:

- `topology_tail_since` returns an **empty overlay outright** when
  `generation.last_wal_id >= last_durable_wal_id`
  (`src/shard/topology_tail.rs:42-44`).
- Otherwise it scans from `generation.last_wal_id + 1`
  (`src/shard/topology_tail.rs:48`), so entries inside WAL file
  `generation.last_wal_id` itself are never examined, and only entries with
  `entry.seq > generation.base_sequence` are collected (`:63`).

The generation's `last_wal_id` comes from `snapshot.last_wal_id().unwrap_or(0)`
(`src/engine/index_store.rs:114`), and on a writer node the durable frontier
falls back to `last_flushed_wal_id()` (`src/core/state.rs:361-364`). Neither
quantity tracks `snapshot.seq()`. If any commit lands in the generation's own
WAL file — or is acknowledged from the memtable before the WAL flushes — it is
in neither the compiled base nor the tail. The predicted symptom is a torn
compiled read: an edge created in the window silently missing, or an
acknowledged delete in the window silently resurrected.

## Reproduction attempts (both pass)

Both in `src/tests.rs`, both `#[ignore]`-marked, both driving the
create → modify → delete interleaving and asserting on the overlay directly.

### 1. Writer-mode generation

`compiled_traversal_reflects_writes_committed_after_the_graph_index_generation`

Builds the generation manifest exactly as `build_graph_index` computes it, then
commits an append and an acknowledged delete after it. The overlay covered both
changes. **Reason it cannot fail here:** a writer snapshot yields
`last_wal_id() == None`, so `unwrap_or(0)` sets `generation.last_wal_id = 0`,
the scan starts at file 1, and it sweeps everything. The hole is closed by
accident on this path, not by design.

### 2. Reader-mode generation (the indexer's shape)

`reader_built_generation_tail_covers_commits_in_its_own_wal_file`

The indexer opens as a reader (`src/bin/graph-indexer.rs:138`), and a reader
snapshot yields `Some(L)` (`src/core/state.rs:102-105`), so the scan starts at
`L + 1` and the boundary is live. A writer cluster commits an append and an
acknowledged delete after the reader builds its generation. The test asserts
`generation.last_wal_id > 0` so it cannot silently degrade into case 1. The
overlay still covered both changes.

## Why neither attempt is a disproof

Both run against `InMemory` object storage, where each commit lands in its own
WAL file, so the batched-commits-in-one-file case the suspect describes never
arose. There is also a structural argument that the boundary may be closed:
a durable reader snapshot's sequence appears to advance over **whole** WAL
files, which would make "entries in file `last_wal_id` with
`seq > base_sequence`" unreachable. That argument is **not verified** — it
rests on SlateDB's durable-snapshot semantics, which were not confirmed against
the SlateDB source.

## What would decide it

1. A delayed-WAL-PUT object-store wrapper that holds flushes open, so several
   commits provably share one WAL file and a snapshot can be taken mid-file.
2. Jepsen, with the indexer in the loop, checking compiled traversal results
   against acknowledged history.

Route 1 is cheaper and should be tried first. Confirm SlateDB's durable
snapshot / WAL-file alignment semantics from source at the same time; that
alone may close this record without any fault injection.

## Not implicated (checked)

- The empty-overlay early return at `:38` (`base_sequence >= read_sequence`) is
  a separate concern, tracked as BFG-013 and disproven there.
- `topology_tail_since` guards `snapshot.seq() != read_sequence` up front
  (`:35-37`) and returns `Unavailable`, so a mismatched snapshot cannot reach
  the window logic at all.

## Fix directions (not applied, and not yet warranted)

No fix should be attempted before the suspect is decided — the current
behaviour may be correct. If it is confirmed, the direction is to derive the
tail's start from the generation's `base_sequence` rather than its
`last_wal_id`, and to stop treating "no newer WAL file" as "no newer commits".
