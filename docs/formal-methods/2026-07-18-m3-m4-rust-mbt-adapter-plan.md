---
title: "Plan: M3 and M4 Rust MBT adapters"
status: implemented
date: 2026-07-18
branch: Turbolay-V3
depends_on:
  - 0005-next-steps-and-completion-gates.md
  - 0003-turbolay-quint-verification-evidence.md
tags: [quint, mbt, quint-connect, plan]
---

# Implemented plan: M3 and M4 Rust MBT adapters

Source: `0005-next-steps-and-completion-gates.md`, ordered work item 2
("Complete M3 and M4 Rust MBT adapters"). Contract decisions (item 1) stay
with the explicit defaults already encoded in the models; no contract change
is proposed here.

## Current state

- Wired Quint Connect drivers: M1 (`tests/formal_mbt.rs`), M2
  (`tests/formal_mbt_m2.rs`), M3 (`tests/formal_mbt_m3.rs`), M4
  (`tests/formal_mbt_m4.rs`), M5 (`tests/formal_mbt_m5.rs`), and P2
  (`tests/formal_mbt_p2.rs`). All run seeded randomized traces against an
  `InMemory` object store and compare a normalized projection after every
  action.
- Implemented: M3 and M4 now have Rust MBT adapters for finite `InMemory`
  replay. This does not claim MinIO/S3 or Jepsen completion.

## Scope

1. `tests/formal_mbt_m3.rs` — new Quint Connect driver for
   `quint-models/turbolay/m3_artifact_gc.qnt`.
2. `tests/formal_mbt_m4.rs` — new Quint Connect driver for
   `quint-models/turbolay/m4_placement_fence.qnt`.
3. One small M3 model refinement (below), with its witness updated.
4. Doc updates: `quint-models/turbolay/README.md`,
   `0003-turbolay-quint-verification-evidence.md`, and the "Current state"
   line in `0005-next-steps-and-completion-gates.md`.

Out of scope (later 0005 items): MinIO/S3 replay, Jepsen campaigns, CI
gates. The drivers use the same seed (`20260718`) so later MinIO runs can
compare failures across storage backends.

## M3 driver: artifact publication, retained readers, GC, equivalence

State projection mirrors every model variable: `epoch`, `previousEpoch`,
`canonicalReachable`, `dirtyGeneration`, `artifactPublished`,
`artifactEpoch`, `artifactReachable`, `builderActive`, `builderBaseEpoch`,
`builderReachable`, `builderGeneration`, `stalePublishRejected`,
`readerActive`, `readerEpoch`, `historyRetained`, `queryResult`,
`lastAction`.

Action binding (public API only):

| Quint action | Rust binding |
|---|---|
| `init` | fresh `InMemory` store + `GraphShard::open_standalone_writer` |
| `writeCreate` / `writeDelete` | `write_edge` / `delete_edge` for edge 1→2; read back `current_epoch` and `edge_exists` (divergence = failure) |
| `startArtifactBuild` | driver-local: capture base epoch, reachable bit, dirty generation |
| `publishCurrentBuild` | `build_matrix_tiles_checked_current(CELL, EDGE, builderBaseEpoch, tile_size)`; verify `latest_matrix_artifact` reports that base epoch |
| `rejectStalePublish` | driver aborts the stale builder (generation changed after copy); assert sticky `stalePublishRejected` |
| `beginRead` / `endRead` | open / drop `OwnedGraphSnapshot` (`shard.owned_snapshot(CELL)`) to pin a reader epoch |
| `gcHistory` | `delete_deltas_through_matrix(CELL, EDGE, artifactEpoch)` |
| `queryMatrix` | `matrix_reachable` **and** `direct_snapshot_reachable` at the current epoch from vertex 1; both must agree, and "2 reachable" must equal `canonicalReachable` — this is the direct/matrix equivalence check |

### Required M3 model refinement

`gcHistory` is currently enabled without a published artifact, but the
implementation refuses delta compaction without a matrix artifact
(`delete_deltas_through_matrix`: "cannot compact deltas without a matrix
artifact"). To keep the driver honest instead of faking collection:

- Add `artifactPublished` to the `gcHistory` guard in
  `m3_artifact_gc.qnt` (models the real contract: history is collectable
  only once an artifact preserves it).
- Update `completedReadPermitsHistoryCollectionTest` in
  `m3_artifact_gc_test.qnt` to `writeCreate → startArtifactBuild →
  publishCurrentBuild` before `gcHistory`.
- Re-run typecheck, deterministic witnesses, and the 6-step Apalache
  `allSafety` check so 0003 evidence stays current.

This follows the precedent of `605fd68` (M1 model touched while wiring its
driver).

## M4 driver: placement disagreement, fencing, takeover, monotone prefix

State projection mirrors every model variable: `durableFence`,
`previousFence`, `node1Candidate`, `node2Candidate`, `node1Active`,
`node2Active`, `node1Reachable`, `node2Reachable`, `node1FencedByNode2`,
`committedPrefix`, `previousPrefix`, `lastAction`.

Binding uses `RoutedGraphCluster` so placement views and the durable writer
fence are both exercised (same mechanism as the existing
`routed_cluster_uses_slatedb_writer_fencing` unit test):

| Quint action | Rust binding |
|---|---|
| `init` | fresh `InMemory` store; no clusters; fence 0; both nodes reachable |
| `chooseNode1` / `chooseNode2` | local placement view `ShardPlacement::fixed([(CELL, "node-1"/"node-2")])`; assert `owner(CELL)` and `ensure_local_owner` agree with the view, and the other node gets `ShardNotOwned` (placement disagreement) |
| `acquireFenceNode1` | `RoutedGraphCluster::open_fenced_owned(path, "node-1", placement, store)`; `node1Active`, fence = 1 |
| `commitNode1` / `commitNode2` | `cluster.write_edge` with a unique idempotency key; assert `current_epoch(CELL)` increments by exactly 1 |
| `partitionNode1` | driver-local `node1Reachable = false`; node 1 cluster stays open (partitioned-but-running old owner) |
| `takeOverFenceNode2` | open fenced cluster as `"node-2"` on the same path + store; fence = 2, `node1Active = false`, `node2Active = true`, and `node1FencedByNode2 = true` |
| `rejectZombieNode1Commit` | require `node1FencedByNode2`, write through the node-1 cluster handle, assert the typed fenced error (`GraphError::Slate`), and verify `current_epoch` is unchanged |

`committedPrefix` maps to `current_epoch(CELL)`; the key check is that the
prefix keeps increasing across the takeover (node 2 continues node 1's epoch
sequence instead of restarting) and never regresses after a rejected zombie
write. No M4 model change is expected.

## Validation

Evidence closure re-runs the complete default-feature Rust MBT adapter set and
repository checks:

1. `cargo test --locked --test formal_mbt -- --test-threads=1`.
2. `cargo test --locked --test formal_mbt_m2 -- --test-threads=1`.
3. `cargo test --locked --test formal_mbt_m3 -- --test-threads=1`.
4. `cargo test --locked --test formal_mbt_m4 -- --test-threads=1`.
5. `cargo test --locked --test formal_mbt_m5 -- --test-threads=1`.
6. `cargo test --locked --test formal_mbt_p2 -- --test-threads=1`.
7. `just fmt-check`.
8. `just check`.

Earlier implementation validation also re-ran the M3/M4 Quint typecheck,
deterministic witnesses, and bounded Apalache checks after the `gcHistory`
model guard refinement.

## Commit

Not committed as part of this documentation/evidence closure task.
