// -*- mode: quint -*-
//
// Implementation-shaped counterpart for BFG-001 and BFG-002.
//
// The bad transitions deliberately model two paths identified in the V2
// analysis: page materialization consults live storage after a snapshot was
// selected, and an unvalidated historical page falls through to current data.
// `quint run` must violate both invariants; this proves the intended M2
// properties discriminate the reported bugs.
module m2_snapshot_read_buggy {
  type GraphView = { epoch: int, rows: List[int] }

  var current: GraphView
  var snapshot: GraphView
  var snapshotOpen: bool
  var page: GraphView
  var pageReturned: bool
  var invalidHistoricalReturned: bool
  var lastAction: str

  pure val initialRows: List[int] = List(1, 2)
  pure val emptyView: GraphView = { epoch: 0, rows: List() }

  action init: bool = all {
    current' = { epoch: 1, rows: initialRows },
    snapshot' = emptyView,
    snapshotOpen' = false,
    page' = emptyView,
    pageReturned' = false,
    invalidHistoricalReturned' = false,
    lastAction' = "init",
  }

  action openSnapshot: bool = all {
    current' = current,
    snapshot' = current,
    snapshotOpen' = true,
    page' = page,
    pageReturned' = pageReturned,
    invalidHistoricalReturned' = invalidHistoricalReturned,
    lastAction' = "openSnapshot",
  }

  action commitAppend: bool = all {
    current.epoch == 1,
    current' = { epoch: 2, rows: current.rows.append(3) },
    snapshot' = snapshot,
    snapshotOpen' = snapshotOpen,
    page' = page,
    pageReturned' = pageReturned,
    invalidHistoricalReturned' = invalidHistoricalReturned,
    lastAction' = "commitAppend",
  }

  // BUG BFG-001: the page path reads the current live view, not `snapshot`.
  action materializePageFromLiveStorage: bool = all {
    snapshotOpen,
    current' = current,
    snapshot' = snapshot,
    snapshotOpen' = snapshotOpen,
    page' = current,
    pageReturned' = true,
    invalidHistoricalReturned' = invalidHistoricalReturned,
    lastAction' = "materializePageFromLiveStorage",
  }

  // BUG BFG-002: an unvalidated historical request returns current data.
  action returnUnvalidatedHistoricalFromCurrent: bool = all {
    not(snapshotOpen),
    current' = current,
    snapshot' = snapshot,
    snapshotOpen' = snapshotOpen,
    page' = current,
    pageReturned' = true,
    invalidHistoricalReturned' = true,
    lastAction' = "returnUnvalidatedHistoricalFromCurrent",
  }

  action step: bool = any {
    openSnapshot,
    commitAppend,
    materializePageFromLiveStorage,
    returnUnvalidatedHistoricalFromCurrent,
  }

  val pageMatchesSnapshot: bool = not(pageReturned) or page == snapshot
  val invalidHistoricalNeverReturns: bool = not(invalidHistoricalReturned)
  val livePageReached: bool = lastAction == "materializePageFromLiveStorage"
  val invalidHistoricalPageReached: bool =
    lastAction == "returnUnvalidatedHistoricalFromCurrent"
}
