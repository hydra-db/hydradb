// -*- mode: quint -*-
//
// M2 — epoch-scoped read stability (BFG-009).
//
// An epoch-scoped read must be a pure function of its epoch: for a fixed epoch
// `e`, `edgeExistsAt(e)` never changes its answer, no matter what commits at
// epochs later than `e`. The implementation evaluates segment visibility
// (`segment.storage_sequence <= read_epoch`, tombstone applies when
// `tombstone_epoch <= read_epoch`) against live storage rather than storage
// pinned at `read_epoch`, while writers destroy the history that evaluation
// depends on.
//
// This module is the intended contract: every write and every tombstone is
// retained, so any epoch can be reconstructed. `m2_epoch_scoped_read_buggy`
// models the two implementation behaviors that break it and must violate these
// invariants.
module m2_epoch_scoped_read {
  // One edge is enough — the property is about one edge's answer at one epoch.
  //
  // `segmentEpochs`   — epochs of segment appends. Appends accumulate.
  // `pointEdgeEpochs` — epochs of point-edge writes. In storage these carry no
  //                     epoch at all, which is what the fault model encodes.
  // `tombstoneEpochs` — epochs of acknowledged deletes. Retained here; the
  //                     implementation deletes tombstone keys on re-append.
  var epoch: int
  var segmentEpochs: Set[int]
  var pointEdgeEpochs: Set[int]
  var tombstoneEpochs: Set[int]

  // The acknowledged history: what the writer was told, and at which epoch.
  var ackedExists: bool
  var ackedEpoch: int

  // What a reader observed, and the epoch it asked about. `observedAt` is 0
  // until an epoch-scoped read has happened.
  var observedAt: int
  var observed: bool
  var firstObservedAt: int
  var firstObserved: bool
  var lastAction: str

  // A write at `w` is superseded at `e` when some delete landed strictly after
  // the write and at or before `e`.
  def supersededAt(w: int, e: int): bool =
    tombstoneEpochs.exists(t => t > w and t <= e)

  def segmentVisibleAt(e: int): bool =
    segmentEpochs.exists(s => s <= e and not(supersededAt(s, e)))

  def pointVisibleAt(e: int): bool =
    pointEdgeEpochs.exists(p => p <= e and not(supersededAt(p, e)))

  def existsAt(e: int): bool = segmentVisibleAt(e) or pointVisibleAt(e)

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

  // Append the edge as a segment. The intended model does NOT delete the
  // tombstone key: history at earlier epochs must survive the re-append.
  action appendSegment: bool = all {
    not(existsAt(epoch)),
    epoch' = epoch + 1,
    segmentEpochs' = segmentEpochs.union(Set(epoch + 1)),
    pointEdgeEpochs' = pointEdgeEpochs,
    tombstoneEpochs' = tombstoneEpochs,
    ackedExists' = true,
    ackedEpoch' = epoch + 1,
    observedAt' = observedAt,
    observed' = observed,
    firstObservedAt' = firstObservedAt,
    firstObserved' = firstObserved,
    lastAction' = "appendSegment",
  }

  // Commit a point edge. The intended model stamps it with its commit epoch so
  // epoch-scoped reads can filter it.
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

  // An epoch-scoped read at the current epoch.
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

  // Re-read the epoch an earlier read already answered. This is the action the
  // stability invariant watches: same epoch, later state, must be same answer.
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

  // A read just happened, so the observation can be judged against the state
  // that produced it.
  val justRead: bool =
    lastAction == "readAtCurrentEpoch" or lastAction == "rereadFirstEpoch"

  // BFG-009 expression 1 — an epoch-scoped read is stable. Re-reading an epoch
  // that was already answered must give the same answer, however many commits
  // landed at later epochs in between.
  val epochScopedReadIsStable: bool =
    not(lastAction == "rereadFirstEpoch") or observed == firstObserved

  // BFG-009 expression 2 — a read never returns data from the future of its
  // epoch. The answer must equal the edge's state reconstructed at that epoch.
  val readNeverSeesTheFuture: bool =
    not(justRead) or observed == existsAt(observedAt)

  // BFG-009 expression 3 — a read anchored at the epoch an operation was
  // acknowledged at must agree with that acknowledgement.
  val readMatchesAcknowledgedHistory: bool =
    not(justRead) or not(observedAt == ackedEpoch) or observed == ackedExists

  val allSafety: bool =
    epochScopedReadIsStable and
      readNeverSeesTheFuture and
      readMatchesAcknowledgedHistory

  // Witnesses. The interleaving that matters is create -> delete -> re-append.
  val deleteAcknowledgedReached: bool =
    lastAction == "deleteEdge" and not(ackedExists)
  val reappendAfterDeleteReached: bool =
    lastAction == "appendSegment" and
      segmentEpochs.exists(s => tombstoneEpochs.exists(t => s > t))
  val rereadAfterReappendReached: bool =
    lastAction == "rereadFirstEpoch" and
      segmentEpochs.exists(s => tombstoneEpochs.exists(t => s > t))
  val pointEdgeAfterReadReached: bool =
    lastAction == "readAtCurrentEpoch" and pointEdgeEpochs.size() > 0
}
