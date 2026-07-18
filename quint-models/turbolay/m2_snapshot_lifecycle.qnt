// -*- mode: quint -*-
//
// M2b — `snapshot_at` admission and cancelled cursor contract.
//
// Turbolay deliberately exposes a storage snapshot only at the current graph
// epoch. A future epoch has no committed state and an older graph epoch is not
// a SlateDB snapshot. Cancellation is terminal for the cursor request: it may
// return an error, but never a page after cancellation.
module m2_snapshot_lifecycle {
  var currentEpoch: int
  var snapshotOpen: bool
  var snapshotEpoch: int
  var futureRejected: bool
  var historicalRejected: bool
  var cursorOpen: bool
  var cursorCancelled: bool
  var pageReturned: bool
  var lastAction: str

  action init: bool = all {
    currentEpoch' = 1,
    snapshotOpen' = false,
    snapshotEpoch' = 0,
    futureRejected' = false,
    historicalRejected' = false,
    cursorOpen' = false,
    cursorCancelled' = false,
    pageReturned' = false,
    lastAction' = "init",
  }

  action openCurrentSnapshot: bool = all {
    currentEpoch' = currentEpoch,
    snapshotOpen' = true,
    snapshotEpoch' = currentEpoch,
    futureRejected' = false,
    historicalRejected' = false,
    cursorOpen' = cursorOpen,
    cursorCancelled' = cursorCancelled,
    pageReturned' = false,
    lastAction' = "openCurrentSnapshot",
  }

  action rejectFutureSnapshot: bool = all {
    currentEpoch' = currentEpoch,
    snapshotOpen' = snapshotOpen,
    snapshotEpoch' = snapshotEpoch,
    futureRejected' = true,
    historicalRejected' = false,
    cursorOpen' = cursorOpen,
    cursorCancelled' = cursorCancelled,
    pageReturned' = false,
    lastAction' = "rejectFutureSnapshot",
  }

  action rejectHistoricalSnapshot: bool = all {
    currentEpoch' = currentEpoch,
    snapshotOpen' = snapshotOpen,
    snapshotEpoch' = snapshotEpoch,
    futureRejected' = false,
    historicalRejected' = true,
    cursorOpen' = cursorOpen,
    cursorCancelled' = cursorCancelled,
    pageReturned' = false,
    lastAction' = "rejectHistoricalSnapshot",
  }

  action openCursor: bool = all {
    snapshotOpen,
    currentEpoch' = currentEpoch,
    snapshotOpen' = snapshotOpen,
    snapshotEpoch' = snapshotEpoch,
    futureRejected' = futureRejected,
    historicalRejected' = historicalRejected,
    cursorOpen' = true,
    cursorCancelled' = false,
    pageReturned' = false,
    lastAction' = "openCursor",
  }

  action cancelCursor: bool = all {
    cursorOpen,
    currentEpoch' = currentEpoch,
    snapshotOpen' = snapshotOpen,
    snapshotEpoch' = snapshotEpoch,
    futureRejected' = futureRejected,
    historicalRejected' = historicalRejected,
    cursorOpen' = false,
    cursorCancelled' = true,
    pageReturned' = false,
    lastAction' = "cancelCursor",
  }

  // This is the observable cancelled-page outcome. It does not manufacture a
  // page after the caller has cancelled the request.
  action rejectFetchAfterCancellation: bool = all {
    cursorCancelled,
    currentEpoch' = currentEpoch,
    snapshotOpen' = snapshotOpen,
    snapshotEpoch' = snapshotEpoch,
    futureRejected' = futureRejected,
    historicalRejected' = historicalRejected,
    cursorOpen' = false,
    cursorCancelled' = cursorCancelled,
    pageReturned' = false,
    lastAction' = "rejectFetchAfterCancellation",
  }

  action fetchCursorPage: bool = all {
    cursorOpen,
    not(cursorCancelled),
    currentEpoch' = currentEpoch,
    snapshotOpen' = snapshotOpen,
    snapshotEpoch' = snapshotEpoch,
    futureRejected' = futureRejected,
    historicalRejected' = historicalRejected,
    cursorOpen' = cursorOpen,
    cursorCancelled' = cursorCancelled,
    pageReturned' = true,
    lastAction' = "fetchCursorPage",
  }

  action step: bool = any {
    openCurrentSnapshot,
    rejectFutureSnapshot,
    rejectHistoricalSnapshot,
    openCursor,
    cancelCursor,
    rejectFetchAfterCancellation,
    fetchCursorPage,
  }

  val openedSnapshotIsCurrent: bool =
    not(snapshotOpen) or snapshotEpoch == currentEpoch
  // Rejection is per request. A later independent current snapshot may quite
  // legitimately produce pages, so only the rejecting transition itself is a
  // no-page outcome.
  val typedSnapshotRejectionHasNoPage: bool =
    not(
      lastAction == "rejectFutureSnapshot" or
        lastAction == "rejectHistoricalSnapshot"
    ) or not(pageReturned)
  val cancelledCursorNeverReturnsPage: bool =
    not(cursorCancelled) or not(pageReturned)
  val cancellationClosesCursor: bool = not(cursorCancelled) or not(cursorOpen)
  val allSafety: bool =
    openedSnapshotIsCurrent and
      typedSnapshotRejectionHasNoPage and
      cancelledCursorNeverReturnsPage and
      cancellationClosesCursor
}
