---
title: Eliminate Redundant Tile Fetches on Concurrent Matrix Adjacency Hydration
status: done
date: 2026-09-21
branch: mm/elim-redundant-tile-fetches
base_commit: 6a2fbb1
head_commit: fbcee7f
tags:
  - matrix-cache
  - thundering-herd
  - concurrency
---

# Eliminate Redundant Tile Fetches on Concurrent Matrix Adjacency Hydration

## Summary
When multiple concurrent read tasks hit an uncached matrix adjacency generation in `cached_matrix_adjacency`, all tasks miss the initial `matrix_cache` lock check. Although they queue on `hydration_gate` (a `Semaphore`), currently the function does not re-check `matrix_cache` after acquiring the permit. As a result, every waiter redundantly fetches, scans, and parses the object-store matrix tiles.

This plan adds a double-checked cache lookup post-permit acquisition in `cached_matrix_adjacency` and a TDD unit test to verify that concurrent callers hit the double-check instead of performing redundant tile hydrations.

## Proposed Changes

### 1. Branch Creation
- Create and switch to git branch `mm/elim-redundant-tile-fetches`.

### 2. TDD Test First (`src/tests.rs`)
- Implement `concurrent_matrix_adjacency_hydration_double_checks_cache()` in `src/tests.rs`:
  - Open a test shard with `max_concurrent_hydrations = 1`.
  - Build a matrix artifact generation.
  - Clear/evict `matrix_cache`.
  - Spawn concurrent tasks calling `shard.cached_matrix_adjacency(...)`.
  - Verify metrics: `matrix_adjacency_hits == 1` and `hydration_completed == 1`.

### 3. Core Fix (`src/engine/matrix_cache.rs`)
- Add a double-checked lookup post-permit in `cached_matrix_adjacency`:
  ```rust
  let _permit = self
      .acquire_hydration_permit("cached_matrix_adjacency")
      .await?;
  if let Some(cached) = self.matrix_cache.lock().await.get(&cache_key) {
      self.cache_metrics
          .record_hit(GraphCacheKind::MatrixAdjacency);
      return Ok(cached);
  }
  ```

## Verification Plan

### Automated Tests
1. `just native-check`
2. `source /tmp/sgk-env.sh 2>/dev/null || true`
3. `cargo test --lib -- concurrent_matrix_adjacency_hydration_double_checks_cache`
4. `just test`
