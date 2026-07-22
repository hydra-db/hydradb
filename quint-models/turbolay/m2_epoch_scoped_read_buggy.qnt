// -*- mode: quint -*-
//
// Implementation-shaped counterpart for BFG-009.
//
// Two bad behaviors are modeled, both taken from the code at a43ec61:
//
//   1. A segment re-append DELETES the tombstone key of a prior acknowledged
//      delete (`src/shard/write.rs:4279`). The record that epoch-scoped
//      evaluation needs is destroyed, so an acknowledged delete is retroactively
//      erased from the epoch at which it was acknowledged.
//
//   2. The point-edge branch of the read composition applies NO epoch filter
//      (`src/engine/artifact_build.rs:497-506`, same shape in
//      `out_neighbors_in_storage_snapshot`, `src/shard/query.rs:135-142`), so a
//      point edge committed after the read epoch is still visible to it.
//
// `quint run` must violate the M2 epoch-scoped invariants here. That is what
// proves the intended properties discriminate the reported bug rather than
// holding vacuously.
module m2_epoch_scoped_read_buggy {
  var epoch: int
  var segmentEpochs: Set[int]
  var pointEdgeEpochs: Set[int]
  var tombstoneEpochs: Set[int]

  var ackedExists: bool
  var ackedEpoch: int

  var observedAt: int
  var observed: bool
  var firstObservedAt: int
  var firstObserved: bool
  var lastAction: str

  def supersededAt(w: int, e: int): bool =
    tombstoneEpochs.exists(t => t > w and t <= e)

  def segmentVisibleAt(e: int): bool =
    segmentEpochs.exists(s => s <= e and not(supersededAt(s, e)))

  // BAD 2 — no epoch filter on the point-edge branch. Any point edge that
  // exists in live storage is visible to every read epoch, including epochs
  // that predate the write.
  def pointVisibleAtUnfiltered(e: int): bool =
    pointEdgeEpochs.exists(p => not(supersededAt(p, e)))

  def existsAt(e: int): bool = segmentVisibleAt(e) or pointVisibleAtUnfiltered(e)

  // The true state, retained alongside so the invariants have something honest
  // to compare against. Storage does not keep this.
  def truePointVisibleAt(e: int): bool =
    pointEdgeEpochs.exists(p => p <= e and not(supersededAt(p, e)))

  action init: bool = all {
    epoch' = 0,
    segmentEpochs' = Set(),
    pointEdgeEpochs' = Set(),
    tombstoneEpochs' = Set(),
    ackedExists' = false,
    ackedEpoch' = 0,
    observedAt' = 0,
    observed' = false,
    firstObservedAt' = 0,
    firstObserved' = false,
    lastAction' = "init",
  }

  // BAD 1 — the re-append deletes every tombstone that precedes it, exactly as
  // `bulk_append_out_adjacency_segment_trusted_txn_locked` deletes the segment
  // tombstone key for each inserted destination.
  action appendSegment: bool = all {
    not(existsAt(epoch)),
    epoch' = epoch + 1,
    segmentEpochs' = segmentEpochs.union(Set(epoch + 1)),
    pointEdgeEpochs' = pointEdgeEpochs,
    tombstoneEpochs' = tombstoneEpochs.filter(t => t > epoch + 1),
    ackedExists' = true,
    ackedEpoch' = epoch + 1,
    observedAt' = observedAt,
    observed' = observed,
    firstObservedAt' = firstObservedAt,
    firstObserved' = firstObserved,
    lastAction' = "appendSegment",
  }

  action writePointEdge: bool = all {
    not(existsAt(epoch)),
    epoch' = epoch + 1,
    segmentEpochs' = segmentEpochs,
    pointEdgeEpochs' = pointEdgeEpochs.union(Set(epoch + 1)),
    tombstoneEpochs' = tombstoneEpochs,
    ackedExists' = true,
    ackedEpoch' = epoch + 1,
    observedAt' = observedAt,
    observed' = observed,
    firstObservedAt' = firstObservedAt,
    firstObserved' = firstObserved,
    lastAction' = "writePointEdge",
  }

  action deleteEdge: bool = all {
    existsAt(epoch),
    epoch' = epoch + 1,
    segmentEpochs' = segmentEpochs,
    pointEdgeEpochs' = pointEdgeEpochs,
    tombstoneEpochs' = tombstoneEpochs.union(Set(epoch + 1)),
    ackedExists' = false,
    ackedEpoch' = epoch + 1,
    observedAt' = observedAt,
    observed' = observed,
    firstObservedAt' = firstObservedAt,
    firstObserved' = firstObserved,
    lastAction' = "deleteEdge",
  }

  action readAtCurrentEpoch: bool = all {
    epoch > 0,
    epoch' = epoch,
    segmentEpochs' = segmentEpochs,
    pointEdgeEpochs' = pointEdgeEpochs,
    tombstoneEpochs' = tombstoneEpochs,
    ackedExists' = ackedExists,
    ackedEpoch' = ackedEpoch,
    observedAt' = epoch,
    observed' = existsAt(epoch),
    firstObservedAt' = if (firstObservedAt == 0) epoch else firstObservedAt,
    firstObserved' = if (firstObservedAt == 0) existsAt(epoch) else firstObserved,
    lastAction' = "readAtCurrentEpoch",
  }

  action rereadFirstEpoch: bool = all {
    firstObservedAt > 0,
    epoch' = epoch,
    segmentEpochs' = segmentEpochs,
    pointEdgeEpochs' = pointEdgeEpochs,
    tombstoneEpochs' = tombstoneEpochs,
    ackedExists' = ackedExists,
    ackedEpoch' = ackedEpoch,
    observedAt' = firstObservedAt,
    observed' = existsAt(firstObservedAt),
    firstObservedAt' = firstObservedAt,
    firstObserved' = firstObserved,
    lastAction' = "rereadFirstEpoch",
  }

  action step: bool = any {
    appendSegment,
    writePointEdge,
    deleteEdge,
    readAtCurrentEpoch,
    rereadFirstEpoch,
  }

  val justRead: bool =
    lastAction == "readAtCurrentEpoch" or lastAction == "rereadFirstEpoch"

  // Same three invariants as the intended module. `readNeverSeesTheFuture`
  // compares against the honest reconstruction, which storage has thrown away.
  val epochScopedReadIsStable: bool =
    not(lastAction == "rereadFirstEpoch") or observed == firstObserved

  val readNeverSeesTheFuture: bool =
    not(justRead) or
      observed == (segmentVisibleAt(observedAt) or truePointVisibleAt(observedAt))

  val readMatchesAcknowledgedHistory: bool =
    not(justRead) or not(observedAt == ackedEpoch) or observed == ackedExists

  val allSafety: bool =
    epochScopedReadIsStable and
      readNeverSeesTheFuture and
      readMatchesAcknowledgedHistory
}
