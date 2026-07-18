// -*- mode: quint -*-
//
// M2 — intended snapshot, cursor, and bookmark contract.
//
// This is the semantic contract for BFG-001, BFG-002, and BFG-008. A storage
// snapshot captures one immutable graph view. A cursor captures that snapshot
// and later writes cannot alter its rows. An unvalidated historical epoch is
// rejected before page dispatch; a bookmark is monotonic within a session.
module m2_snapshot_read {
  type GraphView = { epoch: int, rows: List[int] }

  var current: GraphView
  var snapshot: GraphView
  var snapshotOpen: bool
  var cursor: GraphView
  var cursorOpen: bool
  var cursorOffset: int
  var pageReturned: bool
  var pageIndex: int
  var pageRow: int
  // Direct low-level page tokens are offsets. Each request captures the
  // current view independently, so its observed epoch is not a cursor lease.
  var directCursorOffset: int
  var directPageReturned: bool
  var directPageEpoch: int
  var directPageIndex: int
  var directPageRow: int
  var historicalRejected: bool
  var sessionBookmark: int
  var previousBookmark: int
  var lastAction: str

  pure val initialRows: List[int] = List(1, 2)
  pure val emptyView: GraphView = { epoch: 0, rows: List() }

  action init: bool = all {
    current' = { epoch: 1, rows: initialRows },
    snapshot' = emptyView,
    snapshotOpen' = false,
    cursor' = emptyView,
    cursorOpen' = false,
    cursorOffset' = 0,
    pageReturned' = false,
    pageIndex' = -1,
    pageRow' = 0,
    directCursorOffset' = 0,
    directPageReturned' = false,
    directPageEpoch' = 0,
    directPageIndex' = -1,
    directPageRow' = 0,
    historicalRejected' = false,
    sessionBookmark' = 0,
    previousBookmark' = 0,
    lastAction' = "init",
  }

  // This is the only point at which a current storage view is captured.
  action openSnapshot: bool = all {
    not(cursorOpen),
    snapshot' = current,
    snapshotOpen' = true,
    current' = current,
    cursor' = cursor,
    cursorOpen' = cursorOpen,
    cursorOffset' = cursorOffset,
    pageReturned' = pageReturned,
    pageIndex' = pageIndex,
    pageRow' = pageRow,
    directCursorOffset' = directCursorOffset,
    directPageReturned' = directPageReturned,
    directPageEpoch' = directPageEpoch,
    directPageIndex' = directPageIndex,
    directPageRow' = directPageRow,
    historicalRejected' = historicalRejected,
    sessionBookmark' = sessionBookmark,
    previousBookmark' = previousBookmark,
    lastAction' = "openSnapshot",
  }

  // A successful write moves the current view forward. It deliberately does
  // not mutate a previously captured snapshot or cursor.
  action commitAppend: bool = all {
    current.epoch == 1,
    current' = { epoch: 2, rows: current.rows.append(3) },
    snapshot' = snapshot,
    snapshotOpen' = snapshotOpen,
    cursor' = cursor,
    cursorOpen' = cursorOpen,
    cursorOffset' = cursorOffset,
    pageReturned' = pageReturned,
    pageIndex' = pageIndex,
    pageRow' = pageRow,
    directCursorOffset' = directCursorOffset,
    directPageReturned' = directPageReturned,
    directPageEpoch' = directPageEpoch,
    directPageIndex' = directPageIndex,
    directPageRow' = directPageRow,
    historicalRejected' = historicalRejected,
    sessionBookmark' = sessionBookmark,
    previousBookmark' = previousBookmark,
    lastAction' = "commitAppend",
  }

  action openCursor: bool = all {
    snapshotOpen,
    current' = current,
    snapshot' = snapshot,
    snapshotOpen' = snapshotOpen,
    cursor' = snapshot,
    cursorOpen' = true,
    cursorOffset' = 0,
    pageReturned' = false,
    pageIndex' = -1,
    pageRow' = 0,
    directCursorOffset' = directCursorOffset,
    directPageReturned' = directPageReturned,
    directPageEpoch' = directPageEpoch,
    directPageIndex' = directPageIndex,
    directPageRow' = directPageRow,
    historicalRejected' = historicalRejected,
    sessionBookmark' = sessionBookmark,
    previousBookmark' = previousBookmark,
    lastAction' = "openCursor",
  }

  // Page size is one so the invariant can point at the exact cursor row.
  action fetchCursorPage: bool = all {
    cursorOpen,
    cursorOffset < cursor.rows.length(),
    current' = current,
    snapshot' = snapshot,
    snapshotOpen' = snapshotOpen,
    cursor' = cursor,
    cursorOpen' = cursorOpen,
    cursorOffset' = cursorOffset + 1,
    pageReturned' = true,
    pageIndex' = cursorOffset,
    pageRow' = cursor.rows.nth(cursorOffset),
    directCursorOffset' = directCursorOffset,
    directPageReturned' = directPageReturned,
    directPageEpoch' = directPageEpoch,
    directPageIndex' = directPageIndex,
    directPageRow' = directPageRow,
    historicalRejected' = historicalRejected,
    sessionBookmark' = sessionBookmark,
    previousBookmark' = previousBookmark,
    lastAction' = "fetchCursorPage",
  }

  // The public page entry rejects an epoch unless it came from an actual
  // storage snapshot. No page result is returned by this request.
  action rejectUnvalidatedHistoricalPage: bool = all {
    not(snapshotOpen),
    current' = current,
    snapshot' = snapshot,
    snapshotOpen' = snapshotOpen,
    cursor' = cursor,
    cursorOpen' = cursorOpen,
    cursorOffset' = cursorOffset,
    pageReturned' = false,
    pageIndex' = -1,
    pageRow' = 0,
    directCursorOffset' = directCursorOffset,
    directPageReturned' = directPageReturned,
    directPageEpoch' = directPageEpoch,
    directPageIndex' = directPageIndex,
    directPageRow' = directPageRow,
    historicalRejected' = true,
    sessionBookmark' = sessionBookmark,
    previousBookmark' = previousBookmark,
    lastAction' = "rejectUnvalidatedHistoricalPage",
  }

  // This action represents the forbidden outcome. Keeping it disabled makes
  // the rejection contract deterministic and directly testable.
  action returnUnvalidatedHistoricalPage: bool = all {
    current' = current,
    snapshot' = snapshot,
    snapshotOpen' = snapshotOpen,
    cursor' = cursor,
    cursorOpen' = cursorOpen,
    cursorOffset' = cursorOffset,
    pageReturned' = pageReturned,
    pageIndex' = pageIndex,
    pageRow' = pageRow,
    directCursorOffset' = directCursorOffset,
    directPageReturned' = directPageReturned,
    directPageEpoch' = directPageEpoch,
    directPageIndex' = directPageIndex,
    directPageRow' = directPageRow,
    historicalRejected' = historicalRejected,
    sessionBookmark' = sessionBookmark,
    previousBookmark' = previousBookmark,
    lastAction' = lastAction,
    false,
  }

  action advanceBookmark: bool = all {
    sessionBookmark < current.epoch,
    current' = current,
    snapshot' = snapshot,
    snapshotOpen' = snapshotOpen,
    cursor' = cursor,
    cursorOpen' = cursorOpen,
    cursorOffset' = cursorOffset,
    pageReturned' = pageReturned,
    pageIndex' = pageIndex,
    pageRow' = pageRow,
    directCursorOffset' = directCursorOffset,
    directPageReturned' = directPageReturned,
    directPageEpoch' = directPageEpoch,
    directPageIndex' = directPageIndex,
    directPageRow' = directPageRow,
    historicalRejected' = historicalRejected,
    sessionBookmark' = current.epoch,
    previousBookmark' = sessionBookmark,
    lastAction' = "advanceBookmark",
  }

  // A direct page is a new request. It reads the then-current view at its
  // offset; the token does not pin that view for a later request.
  action fetchDirectPage: bool = all {
    directCursorOffset < current.rows.length(),
    current' = current,
    snapshot' = snapshot,
    snapshotOpen' = snapshotOpen,
    cursor' = cursor,
    cursorOpen' = cursorOpen,
    cursorOffset' = cursorOffset,
    pageReturned' = pageReturned,
    pageIndex' = pageIndex,
    pageRow' = pageRow,
    directCursorOffset' = directCursorOffset + 1,
    directPageReturned' = true,
    directPageEpoch' = current.epoch,
    directPageIndex' = directCursorOffset,
    directPageRow' = current.rows.nth(directCursorOffset),
    historicalRejected' = historicalRejected,
    sessionBookmark' = sessionBookmark,
    previousBookmark' = previousBookmark,
    lastAction' = "fetchDirectPage",
  }

  action step: bool = any {
    openSnapshot,
    commitAppend,
    openCursor,
    fetchCursorPage,
    fetchDirectPage,
    rejectUnvalidatedHistoricalPage,
    advanceBookmark,
  }

  // Every cursor stays bound to the view captured at cursor creation.
  val cursorPinnedToSnapshot: bool =
    not(cursorOpen) or cursor == snapshot

  // A page must be exactly the row from its pinned cursor, never from live
  // storage. `pageIndex` is valid whenever a page was returned.
  val returnedPageMatchesCursor: bool =
    not(pageReturned) or all {
      pageIndex >= 0,
      pageIndex < cursor.rows.length(),
      pageRow == cursor.rows.nth(pageIndex),
    }

  // The intended state machine has no action that may turn an unvalidated
  // historical request into a page result.
  val invalidHistoricalNeverReturns: bool =
    not(lastAction == "returnUnvalidatedHistoricalPage")

  // The prior session bookmark is retained across each action; a session can
  // move forward, never backward.
  val bookmarkMonotone: bool = sessionBookmark >= previousBookmark

  // A direct offset page can only have observed a committed view, but it is
  // deliberately not required to equal the view of a later request.
  val directPageUsesOneRequestView: bool =
    not(directPageReturned) or all {
      directPageEpoch > 0,
      directPageEpoch <= current.epoch,
      directPageIndex >= 0,
    }
  val allSafety: bool =
    cursorPinnedToSnapshot and
      returnedPageMatchesCursor and
      invalidHistoricalNeverReturns and
      bookmarkMonotone and
      directPageUsesOneRequestView

  val pageAfterConcurrentCommitReached: bool =
    pageReturned and current.epoch > cursor.epoch
  val unvalidatedHistoricalRejected: bool =
    lastAction == "rejectUnvalidatedHistoricalPage" and historicalRejected
  val cursorOpenedReached: bool = lastAction == "openCursor"
  val snapshotOpenedReached: bool = lastAction == "openSnapshot"
  val commitAppendReached: bool = lastAction == "commitAppend"
  val cursorPageFetchedReached: bool = lastAction == "fetchCursorPage"
  val directPageFetchedReached: bool = lastAction == "fetchDirectPage"
  val bookmarkAdvancedReached: bool = lastAction == "advanceBookmark"
}
