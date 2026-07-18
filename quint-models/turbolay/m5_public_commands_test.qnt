// -*- mode: quint -*-
module m5_public_commands_test {
  import m5_public_commands.* from "./m5_public_commands"

  run mergeDoesNotDuplicateRelationshipTest =
    init
      .then(createRelationshipCommand)
      .then(mergeSameRelationshipCommand)
      .expect(createdRelationshipCount == 1)
      .expect(lastResponse == "matched")

  run ambiguousRelationshipIdIsExplicitlyRejectedTest =
    init
      .then(createRelationshipCommand)
      .then(rejectAmbiguousRelationshipId)
      .expect(ambiguousRelationshipRejected)
      .expect(createdRelationshipCount == 1)
      .expect(relationshipDst == 2)

  run conflictingDuplicateVertexIsRejectedTest =
    init
      .then(rejectConflictingDuplicateVertex)
      .expect(duplicateVertexRejected)
      .expect(vertexPresent)

  run serverCursorConsumesMaterializedRowsTest =
    init
      .then(openMaterializedCursor)
      .then(fetchMaterializedCursorPage)
      .expect(cursorOffset == 1)
      .expect(cursorOpen)
      .then(fetchMaterializedCursorPage)
      .expect(cursorOffset == 2)
      .expect(not(cursorOpen))

  run createEdgeCommandHasPublicCreatedOutcomeTest =
    init
      .then(createEdgeCommand)
      .expect(edgePresent)
      .expect(lastResponse == "created")

  run edgeMetadataDoesNotChangeStructuralAdjacencyTest =
    init
      .then(createEdgeCommand)
      .then(setEdgeMetadataCommand)
      .expect(edgePresent)
      .expect(edgeMetadata == "set")
      .then(removeEdgeMetadataCommand)
      .expect(edgePresent)
      .expect(edgeMetadata == "none")

  run parallelRelationshipDeletePreservesEdgeUntilFinalDeleteTest =
    init
      .then(createRelationshipCommand)
      .then(createParallelRelationshipCommand)
      .expect(liveRelationshipCount == 2)
      .then(deleteOneParallelRelationshipCommand)
      .expect(liveRelationshipCount == 1)
      .expect(edgePresent)
      .then(deleteFinalRelationshipCommand)
      .expect(liveRelationshipCount == 0)
      .expect(not(edgePresent))
}
