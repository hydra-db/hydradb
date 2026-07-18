// -*- mode: quint -*-
// Deterministic M2 contract scenarios. Main model remains scenario-free.
module m2_snapshot_read_test {
  import m2_snapshot_read.* from "./m2_snapshot_read"

  run cursorRemainsPinnedAcrossCommitTest =
    init
      .then(openSnapshot)
      .then(openCursor)
      .then(commitAppend)
      .then(fetchCursorPage)
      .expect(pageRow == 1)
      .then(fetchCursorPage)
      .expect(pageRow == 2)
      .expect(cursor.epoch == 1)
      .expect(current.epoch == 2)

  run unvalidatedHistoricalPageIsRejectedTest =
    init
      .then(rejectUnvalidatedHistoricalPage)
      .expect(historicalRejected)
      .expect(not(pageReturned))

  run bookmarkDoesNotRegressTest =
    init
      .then(advanceBookmark)
      .expect(sessionBookmark == 1)
      .then(commitAppend)
      .then(advanceBookmark)
      .expect(sessionBookmark == 2)
      .expect(previousBookmark == 1)

  run directOffsetPaginationUsesItsOwnRequestViewTest =
    init
      .then(fetchDirectPage)
      .expect(directPageEpoch == 1)
      .expect(directPageIndex == 0)
      .expect(directPageRow == 1)
      .then(commitAppend)
      .then(fetchDirectPage)
      .expect(directPageEpoch == 2)
      .expect(directPageIndex == 1)
      .expect(directPageRow == 2)
}
