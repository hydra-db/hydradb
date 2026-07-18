// -*- mode: quint -*-
module m1_cell_write_test {
  import m1_cell_write.* from "./m1_cell_write"

  run lostReplyRetryIsExactlyOnceTest =
    init
      .then(openWriter1)
      .then(commitThenLoseReply)
      .expect(edgePresent)
      .expect(epoch == 1)
      .then(retryCreate)
      .expect(edgePresent)
      .expect(epoch == 1)
      .expect(outDegree == 1)

  run deleteRetryDoesNotAdvanceEpochTest =
    init
      .then(openWriter1)
      .then(createEdge)
      .then(deleteEdge)
      .expect(not(edgePresent))
      .expect(epoch == 2)
      .then(retryDelete)
      .expect(epoch == 2)
      .expect(outDegree == 0)

  run staleWriterIsFencedAfterTakeoverTest =
    init
      .then(openWriter1)
      .then(createEdge)
      .then(crashWriter1)
      .then(takeOverWriter2)
      .then(rejectZombieWrite)
      .expect(activeWriter == 2)
      .expect(edgePresent)
      .expect(epoch == 1)
}
