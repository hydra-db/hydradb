// -*- mode: quint -*-
//
// M1 — durable per-cell mutation, idempotency, and writer-fence contract.
//
// This deliberately models one structural edge. `edgePresent`, `outDegree`,
// and `deltaEpoch` are the normalized projections which must commit together.
// A real Turbolay mutation has richer records and indexes, but any successful
// mutation must refine this atomic transition.
module m1_cell_write {
  var epoch: int
  var previousEpoch: int
  var edgePresent: bool
  var outDegree: int
  var deltaEpoch: int
  var createRecorded: bool
  var deleteRecorded: bool
  var createOutcomeEpoch: int
  var deleteOutcomeEpoch: int
  var unknownReply: bool
  var activeWriter: int
  var writer1Live: bool
  var writer2Live: bool
  var lastAction: str

  action init: bool = all {
    epoch' = 0,
    previousEpoch' = 0,
    edgePresent' = false,
    outDegree' = 0,
    deltaEpoch' = 0,
    createRecorded' = false,
    deleteRecorded' = false,
    createOutcomeEpoch' = 0,
    deleteOutcomeEpoch' = 0,
    unknownReply' = false,
    activeWriter' = 0,
    writer1Live' = false,
    writer2Live' = false,
    lastAction' = "init",
  }

  action openWriter1: bool = all {
    activeWriter == 0,
    epoch' = epoch,
    previousEpoch' = epoch,
    edgePresent' = edgePresent,
    outDegree' = outDegree,
    deltaEpoch' = deltaEpoch,
    createRecorded' = createRecorded,
    deleteRecorded' = deleteRecorded,
    createOutcomeEpoch' = createOutcomeEpoch,
    deleteOutcomeEpoch' = deleteOutcomeEpoch,
    unknownReply' = unknownReply,
    activeWriter' = 1,
    writer1Live' = true,
    writer2Live' = writer2Live,
    lastAction' = "openWriter1",
  }

  // CREATE commits its current edge projection, topology delta, and outcome
  // as one durable state transition.
  action createEdge: bool = all {
    activeWriter == 1,
    writer1Live,
    not(edgePresent),
    not(createRecorded),
    epoch' = epoch + 1,
    previousEpoch' = epoch,
    edgePresent' = true,
    outDegree' = 1,
    deltaEpoch' = epoch + 1,
    createRecorded' = true,
    deleteRecorded' = deleteRecorded,
    createOutcomeEpoch' = epoch + 1,
    deleteOutcomeEpoch' = deleteOutcomeEpoch,
    unknownReply' = false,
    activeWriter' = activeWriter,
    writer1Live' = writer1Live,
    writer2Live' = writer2Live,
    lastAction' = "createEdge",
  }

  // The write committed but the caller did not receive its acknowledgement.
  action commitThenLoseReply: bool = all {
    activeWriter == 1,
    writer1Live,
    not(edgePresent),
    not(createRecorded),
    epoch' = epoch + 1,
    previousEpoch' = epoch,
    edgePresent' = true,
    outDegree' = 1,
    deltaEpoch' = epoch + 1,
    createRecorded' = true,
    deleteRecorded' = deleteRecorded,
    createOutcomeEpoch' = epoch + 1,
    deleteOutcomeEpoch' = deleteOutcomeEpoch,
    unknownReply' = true,
    activeWriter' = activeWriter,
    writer1Live' = writer1Live,
    writer2Live' = writer2Live,
    lastAction' = "commitThenLoseReply",
  }

  // A matching create retry returns its recorded outcome and does not write.
  action retryCreate: bool = all {
    createRecorded,
    activeWriter != 0,
    epoch' = epoch,
    previousEpoch' = epoch,
    edgePresent' = edgePresent,
    outDegree' = outDegree,
    deltaEpoch' = deltaEpoch,
    createRecorded' = createRecorded,
    deleteRecorded' = deleteRecorded,
    createOutcomeEpoch' = createOutcomeEpoch,
    deleteOutcomeEpoch' = deleteOutcomeEpoch,
    unknownReply' = false,
    activeWriter' = activeWriter,
    writer1Live' = writer1Live,
    writer2Live' = writer2Live,
    lastAction' = "retryCreate",
  }

  // A reused idempotency key with a different mutation is rejected.
  action rejectConflictingRetry: bool = all {
    createRecorded,
    activeWriter != 0,
    epoch' = epoch,
    previousEpoch' = epoch,
    edgePresent' = edgePresent,
    outDegree' = outDegree,
    deltaEpoch' = deltaEpoch,
    createRecorded' = createRecorded,
    deleteRecorded' = deleteRecorded,
    createOutcomeEpoch' = createOutcomeEpoch,
    deleteOutcomeEpoch' = deleteOutcomeEpoch,
    unknownReply' = unknownReply,
    activeWriter' = activeWriter,
    writer1Live' = writer1Live,
    writer2Live' = writer2Live,
    lastAction' = "rejectConflictingRetry",
  }

  action deleteEdge: bool = all {
    activeWriter == 1,
    writer1Live,
    edgePresent,
    not(deleteRecorded),
    epoch' = epoch + 1,
    previousEpoch' = epoch,
    edgePresent' = false,
    outDegree' = 0,
    deltaEpoch' = epoch + 1,
    createRecorded' = createRecorded,
    deleteRecorded' = true,
    createOutcomeEpoch' = createOutcomeEpoch,
    deleteOutcomeEpoch' = epoch + 1,
    unknownReply' = false,
    activeWriter' = activeWriter,
    writer1Live' = writer1Live,
    writer2Live' = writer2Live,
    lastAction' = "deleteEdge",
  }

  action retryDelete: bool = all {
    deleteRecorded,
    activeWriter != 0,
    epoch' = epoch,
    previousEpoch' = epoch,
    edgePresent' = edgePresent,
    outDegree' = outDegree,
    deltaEpoch' = deltaEpoch,
    createRecorded' = createRecorded,
    deleteRecorded' = deleteRecorded,
    createOutcomeEpoch' = createOutcomeEpoch,
    deleteOutcomeEpoch' = deleteOutcomeEpoch,
    unknownReply' = false,
    activeWriter' = activeWriter,
    writer1Live' = writer1Live,
    writer2Live' = writer2Live,
    lastAction' = "retryDelete",
  }

  action crashWriter1: bool = all {
    activeWriter == 1,
    writer1Live,
    epoch' = epoch,
    previousEpoch' = epoch,
    edgePresent' = edgePresent,
    outDegree' = outDegree,
    deltaEpoch' = deltaEpoch,
    createRecorded' = createRecorded,
    deleteRecorded' = deleteRecorded,
    createOutcomeEpoch' = createOutcomeEpoch,
    deleteOutcomeEpoch' = deleteOutcomeEpoch,
    unknownReply' = unknownReply,
    activeWriter' = 0,
    writer1Live' = false,
    writer2Live' = writer2Live,
    lastAction' = "crashWriter1",
  }

  action takeOverWriter2: bool = all {
    activeWriter == 0,
    not(writer1Live),
    lastAction == "crashWriter1",
    epoch' = epoch,
    previousEpoch' = epoch,
    edgePresent' = edgePresent,
    outDegree' = outDegree,
    deltaEpoch' = deltaEpoch,
    createRecorded' = createRecorded,
    deleteRecorded' = deleteRecorded,
    createOutcomeEpoch' = createOutcomeEpoch,
    deleteOutcomeEpoch' = deleteOutcomeEpoch,
    unknownReply' = unknownReply,
    activeWriter' = 2,
    writer1Live' = false,
    writer2Live' = true,
    lastAction' = "takeOverWriter2",
  }

  // A fenced former writer is explicitly observable as a rejected attempt,
  // never as a second commit.
  action rejectZombieWrite: bool = all {
    activeWriter == 2,
    not(writer1Live),
    epoch' = epoch,
    previousEpoch' = epoch,
    edgePresent' = edgePresent,
    outDegree' = outDegree,
    deltaEpoch' = deltaEpoch,
    createRecorded' = createRecorded,
    deleteRecorded' = deleteRecorded,
    createOutcomeEpoch' = createOutcomeEpoch,
    deleteOutcomeEpoch' = deleteOutcomeEpoch,
    unknownReply' = unknownReply,
    activeWriter' = activeWriter,
    writer1Live' = writer1Live,
    writer2Live' = writer2Live,
    lastAction' = "rejectZombieWrite",
  }

  action step: bool = any {
    openWriter1,
    createEdge,
    commitThenLoseReply,
    retryCreate,
    rejectConflictingRetry,
    deleteEdge,
    retryDelete,
    crashWriter1,
    takeOverWriter2,
    rejectZombieWrite,
  }

  val epochNeverRegresses: bool = epoch >= previousEpoch
  val edgeProjectionConsistent: bool =
    (edgePresent and outDegree == 1) or (not(edgePresent) and outDegree == 0)
  val deltaMatchesLatestTopologyChange: bool = deltaEpoch == epoch
  val createIdempotencyExact: bool =
    not(createRecorded) or all { createOutcomeEpoch > 0, createOutcomeEpoch <= epoch }
  val deleteIdempotencyExact: bool =
    not(deleteRecorded) or all { deleteOutcomeEpoch > 0, deleteOutcomeEpoch <= epoch }
  val oneEffectiveWriter: bool =
    not(writer1Live and writer2Live) and
      ((activeWriter == 1) implies writer1Live) and
      ((activeWriter == 2) implies writer2Live)
  val zombieWriteRejected: bool = not(lastAction == "zombieWrite")
  val allSafety: bool =
    epochNeverRegresses and
      edgeProjectionConsistent and
      deltaMatchesLatestTopologyChange and
      createIdempotencyExact and
      deleteIdempotencyExact and
      oneEffectiveWriter and
      zombieWriteRejected

  val lostReplyReached: bool = lastAction == "commitThenLoseReply"
  val takeoverReached: bool = lastAction == "takeOverWriter2"
  val zombieRejectionReached: bool = lastAction == "rejectZombieWrite"
  val openWriter1Reached: bool = lastAction == "openWriter1"
  val createEdgeReached: bool = lastAction == "createEdge"
  val retryCreateReached: bool = lastAction == "retryCreate"
  val conflictingRetryRejectionReached: bool = lastAction == "rejectConflictingRetry"
  val deleteEdgeReached: bool = lastAction == "deleteEdge"
  val retryDeleteReached: bool = lastAction == "retryDelete"
  val crashWriter1Reached: bool = lastAction == "crashWriter1"
}
