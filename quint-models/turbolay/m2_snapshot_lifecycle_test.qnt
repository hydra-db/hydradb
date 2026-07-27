// -*- mode: quint -*-
module m2_snapshot_lifecycle_test {
  import m2_snapshot_lifecycle.* from "./m2_snapshot_lifecycle"

  run currentEpochSnapshotIsUsableTest =
    init
      .then(openCurrentSnapshot)
      .expect(snapshotOpen)
      .expect(snapshotEpoch == currentEpoch)
      .then(openCursor)
      .then(fetchCursorPage)
      .expect(pageReturned)

  run futureAndHistoricalEpochsAreTypedNoPageRejectionsTest =
    init
      .then(rejectFutureSnapshot)
      .expect(futureRejected)
      .expect(not(pageReturned))
      .then(rejectHistoricalSnapshot)
      .expect(historicalRejected)
      .expect(not(pageReturned))

  run cancelledCursorCannotProduceAnotherPageTest =
    init
      .then(openCurrentSnapshot)
      .then(openCursor)
      .then(cancelCursor)
      .expect(cursorCancelled)
      .expect(not(cursorOpen))
      .then(rejectFetchAfterCancellation)
      .expect(not(pageReturned))
}
