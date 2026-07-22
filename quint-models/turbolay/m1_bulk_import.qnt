// -*- mode: quint -*-
//
// M1b — chunked bulk-import unit and retry contract.
//
// A single user request has five distinct edges and is admitted as 2 + 2 + 1
// chunks. A completed chunk is durable as a unit. A failure after a completed
// chunk leaves only that durable prefix; retrying an already completed chunk or
// the complete request is observationally idempotent.
module m1_bulk_import {
  var epoch: int
  var previousEpoch: int
  var insertedEdges: int
  var completedChunks: int
  var requestCompleted: bool
  var retryObserved: bool
  var invalidChunkRejected: bool
  var lastAction: str

  action init: bool = all {
    epoch' = 0,
    previousEpoch' = 0,
    insertedEdges' = 0,
    completedChunks' = 0,
    requestCompleted' = false,
    retryObserved' = false,
    invalidChunkRejected' = false,
    lastAction' = "init",
  }

  // A two-edge chunk is durable before the caller may start the next one.
  action commitFirstChunk: bool = all {
    completedChunks == 0,
    epoch' = epoch + 2,
    previousEpoch' = epoch,
    insertedEdges' = 2,
    completedChunks' = 1,
    requestCompleted' = false,
    retryObserved' = retryObserved,
    invalidChunkRejected' = invalidChunkRejected,
    lastAction' = "commitFirstChunk",
  }

  // The client can lose progress after a durable chunk without inventing a
  // partial third edge or advancing the graph epoch.
  action failBetweenChunks: bool = all {
    completedChunks == 1,
    epoch' = epoch,
    previousEpoch' = epoch,
    insertedEdges' = insertedEdges,
    completedChunks' = completedChunks,
    requestCompleted' = requestCompleted,
    retryObserved' = retryObserved,
    invalidChunkRejected' = invalidChunkRejected,
    lastAction' = "failBetweenChunks",
  }

  action retryFirstChunk: bool = all {
    completedChunks >= 1,
    epoch' = epoch,
    previousEpoch' = epoch,
    insertedEdges' = insertedEdges,
    completedChunks' = completedChunks,
    requestCompleted' = requestCompleted,
    retryObserved' = true,
    invalidChunkRejected' = invalidChunkRejected,
    lastAction' = "retryFirstChunk",
  }

  action commitSecondChunk: bool = all {
    completedChunks == 1,
    epoch' = epoch + 2,
    previousEpoch' = epoch,
    insertedEdges' = 4,
    completedChunks' = 2,
    requestCompleted' = false,
    retryObserved' = retryObserved,
    invalidChunkRejected' = invalidChunkRejected,
    lastAction' = "commitSecondChunk",
  }

  action commitFinalChunk: bool = all {
    completedChunks == 2,
    epoch' = epoch + 1,
    previousEpoch' = epoch,
    insertedEdges' = 5,
    completedChunks' = 3,
    requestCompleted' = true,
    retryObserved' = retryObserved,
    invalidChunkRejected' = invalidChunkRejected,
    lastAction' = "commitFinalChunk",
  }

  action retryWholeRequest: bool = all {
    requestCompleted,
    epoch' = epoch,
    previousEpoch' = epoch,
    insertedEdges' = insertedEdges,
    completedChunks' = completedChunks,
    requestCompleted' = requestCompleted,
    retryObserved' = true,
    invalidChunkRejected' = invalidChunkRejected,
    lastAction' = "retryWholeRequest",
  }

  action rejectInvalidChunkSize: bool = all {
    epoch' = epoch,
    previousEpoch' = epoch,
    insertedEdges' = insertedEdges,
    completedChunks' = completedChunks,
    requestCompleted' = requestCompleted,
    retryObserved' = retryObserved,
    invalidChunkRejected' = true,
    lastAction' = "rejectInvalidChunkSize",
  }

  action step: bool = any {
    commitFirstChunk,
    failBetweenChunks,
    retryFirstChunk,
    commitSecondChunk,
    commitFinalChunk,
    retryWholeRequest,
    rejectInvalidChunkSize,
  }

  val epochNeverRegresses: bool = epoch >= previousEpoch
  val chunkPrefixIsAtomic: bool =
    (completedChunks == 0 and insertedEdges == 0 and epoch == 0) or
      (completedChunks == 1 and insertedEdges == 2 and epoch == 2) or
      (completedChunks == 2 and insertedEdges == 4 and epoch == 4) or
      (completedChunks == 3 and insertedEdges == 5 and epoch == 5)
  val completionMatchesAllChunks: bool = requestCompleted == (completedChunks == 3)
  val retryDoesNotCreateDuplicates: bool =
    not(retryObserved) or insertedEdges <= 5
  val invalidChunkHasNoWrite: bool =
    not(lastAction == "rejectInvalidChunkSize") or epoch == previousEpoch
  val allSafety: bool =
    epochNeverRegresses and
      chunkPrefixIsAtomic and
      completionMatchesAllChunks and
      retryDoesNotCreateDuplicates and
      invalidChunkHasNoWrite

  val failureBoundaryReached: bool = lastAction == "failBetweenChunks"
  val wholeRetryReached: bool = lastAction == "retryWholeRequest"
  val invalidChunkRejectedReached: bool =
    lastAction == "rejectInvalidChunkSize" and invalidChunkRejected
}
