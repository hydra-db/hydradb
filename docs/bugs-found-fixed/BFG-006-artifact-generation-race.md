---
id: BFG-006
title: Artifact publication and dirty-generation clearing may race a write
status: discovered
severity: P2
classification: concurrency-risk
introduced_or_first_bad_commit: pending-bisect
fix_commit: none
model: quint-models/turbolay/m3_artifact_gc.qnt
current_verified_commit: f45662c
date_opened: 2026-07-18
date_verified: null
tags: [bugs, artifacts, generation, gc, concurrency, quint]
---

# BFG-006: artifact generation race

## Status

Discovered as a concurrency risk. A builder that captured generation `G` must not publish an artifact or clear a dirty marker after a topology write advances the generation beyond `G`; no historical code-level race has been forced yet.

## Intended behavior

Publication is conditional on the builder's source generation still being current. Otherwise the builder aborts/retries. A query at a snapshot must equal the direct canonical result whether it uses a fresh artifact plus deltas or the fallback path. GC must retain data referenced by active reads.

## Reproduction to add

Pause a builder after it reads the base generation, commit a topology write, then resume publication/dirty clear. Run this at the historical target and the current head; query direct and matrix paths from a fresh snapshot.

## Impact

A stale publish can erase the signal to rebuild, omit a delta, or leave an incorrect artifact visible after a successful write.

## Formal coverage and next step

M3 encodes start-build → write → `rejectStalePublish`; its deterministic test, simulation, and six-step Apalache check pass. The missing piece is an injected implementation interleaving and a MinIO/Jepsen maintenance campaign.

## Review decision

Pending a forced race reproduction; this record is not evidence that current artifact fencing is correct under all schedules.
