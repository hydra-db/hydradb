// -*- mode: quint -*-
module m1_bulk_import_test {
  import m1_bulk_import.* from "./m1_bulk_import"

  run failureAfterChunkRetainsOnlyDurablePrefixTest =
    init
      .then(commitFirstChunk)
      .then(failBetweenChunks)
      .expect(insertedEdges == 2)
      .expect(completedChunks == 1)
      .expect(epoch == 2)
      .then(retryFirstChunk)
      .expect(insertedEdges == 2)
      .expect(epoch == 2)

  run completedChunkedRequestIsIdempotentTest =
    init
      .then(commitFirstChunk)
      .then(commitSecondChunk)
      .then(commitFinalChunk)
      .expect(requestCompleted)
      .expect(insertedEdges == 5)
      .expect(epoch == 5)
      .then(retryWholeRequest)
      .expect(insertedEdges == 5)
      .expect(epoch == 5)

  run invalidChunkSizeDoesNotWriteTest =
    init
      .then(rejectInvalidChunkSize)
      .expect(invalidChunkRejected)
      .expect(epoch == 0)
      .expect(insertedEdges == 0)
}
