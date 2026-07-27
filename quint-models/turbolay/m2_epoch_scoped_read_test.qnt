// -*- mode: quint -*-
// Deterministic BFG-009 scenarios. Main model remains scenario-free.
//
// Each run mirrors one of the three e2e repros in `src/tests.rs`. Under the
// intended contract every one of them holds.
//
// Under `m2_epoch_scoped_read_buggy` the first two interleavings violate their
// invariants (`epochScopedReadIsStable`, `readNeverSeesTheFuture`). The third
// does NOT: `readMatchesAcknowledgedHistory` is not discriminated by that
// module, because a read here is atomic — the epoch is derived and evaluated in
// one action — whereas the e2e repro depends on a writer committing between
// `query_read_epoch` and the read. See BFG-009, "Formal model".
module m2_epoch_scoped_read_test {
  import m2_epoch_scoped_read.* from "./m2_epoch_scoped_read"

  // Repro 1 — an acknowledged delete must not be retroactively erased.
  // create -> delete -> read@delete-epoch -> re-append -> re-read@delete-epoch
  //
  // The re-append is what deletes the tombstone key in the implementation. The
  // answer at the delete epoch must not move.
  run acknowledgedDeleteSurvivesReappendTest =
    init
      .then(appendSegment)
      .expect(ackedEpoch == 1 and ackedExists)
      .then(deleteEdge)
      .expect(ackedEpoch == 2 and not(ackedExists))
      .then(readAtCurrentEpoch)
      .expect(observedAt == 2 and not(observed))
      .expect(firstObservedAt == 2 and not(firstObserved))
      .then(appendSegment)
      .expect(epoch == 3)
      .then(rereadFirstEpoch)
      .expect(observedAt == 2)
      .expect(not(observed))
      .expect(epochScopedReadIsStable)

  // Repro 2 — a read must not return an edge committed after its epoch.
  // create -> delete -> read@2 -> point-write@3 -> re-read@2
  //
  // The point-edge branch carries no epoch filter in the implementation, so the
  // epoch-3 write leaks into the epoch-2 answer.
  run readDoesNotSeeAPointEdgeFromItsFutureTest =
    init
      .then(appendSegment)
      .then(deleteEdge)
      .then(readAtCurrentEpoch)
      .expect(observedAt == 2 and not(observed))
      .then(writePointEdge)
      .expect(epoch == 3)
      .expect(pointEdgeEpochs == Set(3))
      .then(rereadFirstEpoch)
      .expect(observedAt == 2)
      .expect(not(observed))
      .expect(readNeverSeesTheFuture)

  // Repro 3 — a read anchored at an acknowledged epoch must agree with that
  // acknowledgement, across a full create -> delete -> re-append cycle.
  run currentEpochReadMatchesAcknowledgedHistoryTest =
    init
      .then(appendSegment)
      .then(readAtCurrentEpoch)
      .expect(observed and observedAt == ackedEpoch)
      .then(deleteEdge)
      .then(readAtCurrentEpoch)
      .expect(not(observed) and observedAt == ackedEpoch)
      .then(appendSegment)
      .then(readAtCurrentEpoch)
      .expect(observed and observedAt == ackedEpoch)
      .expect(readMatchesAcknowledgedHistory)

  // The point-edge write is epoch-stamped, so it is invisible to earlier
  // epochs and visible from its own epoch onward.
  run pointEdgeIsScopedToItsCommitEpochTest =
    init
      .then(writePointEdge)
      .expect(pointEdgeEpochs == Set(1))
      .expect(pointVisibleAt(1))
      .expect(not(pointVisibleAt(0)))
      .then(readAtCurrentEpoch)
      .expect(observed and observedAt == 1)
}
