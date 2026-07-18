// -*- mode: quint -*-
//
// M5 — small public-command refinement model.
//
// It intentionally does not parse Cypher. Each action is the normalized
// behavior of one supported command class that an MBT adapter will invoke.
// The relationship-ID behavior is a provisional safety contract: an ambiguous
// duplicate external ID is rejected until the product approves its scope.
module m5_public_commands {
  var edgePresent: bool
  var vertexPresent: bool
  var relationshipPresent: bool
  var relationshipId: int
  var relationshipSrc: int
  var relationshipDst: int
  var createdRelationshipCount: int
  var edgeMetadata: str
  var duplicateVertexRejected: bool
  var ambiguousRelationshipRejected: bool
  var materializedRows: int
  var cursorOffset: int
  var cursorOpen: bool
  var lastResponse: str
  var lastAction: str

  action init: bool = all {
    edgePresent' = false,
    vertexPresent' = true,
    relationshipPresent' = false,
    relationshipId' = 0,
    relationshipSrc' = 0,
    relationshipDst' = 0,
    createdRelationshipCount' = 0,
    edgeMetadata' = "none",
    duplicateVertexRejected' = false,
    ambiguousRelationshipRejected' = false,
    materializedRows' = 0,
    cursorOffset' = 0,
    cursorOpen' = false,
    lastResponse' = "ok",
    lastAction' = "init",
  }

  action createEdgeCommand: bool = all {
    not(edgePresent),
    edgePresent' = true,
    vertexPresent' = vertexPresent,
    relationshipPresent' = relationshipPresent,
    relationshipId' = relationshipId,
    relationshipSrc' = relationshipSrc,
    relationshipDst' = relationshipDst,
    createdRelationshipCount' = createdRelationshipCount,
    edgeMetadata' = edgeMetadata,
    duplicateVertexRejected' = duplicateVertexRejected,
    ambiguousRelationshipRejected' = ambiguousRelationshipRejected,
    materializedRows' = materializedRows,
    cursorOffset' = cursorOffset,
    cursorOpen' = cursorOpen,
    lastResponse' = "created",
    lastAction' = "createEdgeCommand",
  }

  // CREATE allocates a relationship identity. The finite model uses ID 7.
  action createRelationshipCommand: bool = all {
    not(relationshipPresent),
    edgePresent' = true,
    vertexPresent' = vertexPresent,
    relationshipPresent' = true,
    relationshipId' = 7,
    relationshipSrc' = 1,
    relationshipDst' = 2,
    createdRelationshipCount' = createdRelationshipCount + 1,
    edgeMetadata' = edgeMetadata,
    duplicateVertexRejected' = duplicateVertexRejected,
    ambiguousRelationshipRejected' = ambiguousRelationshipRejected,
    materializedRows' = materializedRows,
    cursorOffset' = cursorOffset,
    cursorOpen' = cursorOpen,
    lastResponse' = "created",
    lastAction' = "createRelationshipCommand",
  }

  // MERGE of the same immutable identity returns the existing relationship.
  action mergeSameRelationshipCommand: bool = all {
    relationshipPresent,
    relationshipId == 7,
    relationshipSrc == 1,
    relationshipDst == 2,
    edgePresent' = edgePresent,
    vertexPresent' = vertexPresent,
    relationshipPresent' = relationshipPresent,
    relationshipId' = relationshipId,
    relationshipSrc' = relationshipSrc,
    relationshipDst' = relationshipDst,
    createdRelationshipCount' = createdRelationshipCount,
    edgeMetadata' = edgeMetadata,
    duplicateVertexRejected' = duplicateVertexRejected,
    ambiguousRelationshipRejected' = ambiguousRelationshipRejected,
    materializedRows' = materializedRows,
    cursorOffset' = cursorOffset,
    cursorOpen' = cursorOpen,
    lastResponse' = "matched",
    lastAction' = "mergeSameRelationshipCommand",
  }

  // Same external ID at another endpoint is not silently aliased. Whether the
  // eventual public contract rejects or scopes it by endpoint is a review
  // decision; silently merging it is never an allowed action.
  action rejectAmbiguousRelationshipId: bool = all {
    relationshipPresent,
    relationshipId == 7,
    edgePresent' = edgePresent,
    vertexPresent' = vertexPresent,
    relationshipPresent' = relationshipPresent,
    relationshipId' = relationshipId,
    relationshipSrc' = relationshipSrc,
    relationshipDst' = relationshipDst,
    createdRelationshipCount' = createdRelationshipCount,
    edgeMetadata' = edgeMetadata,
    duplicateVertexRejected' = duplicateVertexRejected,
    ambiguousRelationshipRejected' = true,
    materializedRows' = materializedRows,
    cursorOffset' = cursorOffset,
    cursorOpen' = cursorOpen,
    lastResponse' = "rejected",
    lastAction' = "rejectAmbiguousRelationshipId",
  }

  // The currently observed batch behavior is explicit: conflicting values for
  // one vertex row reject the batch rather than applying an arbitrary winner.
  action rejectConflictingDuplicateVertex: bool = all {
    vertexPresent,
    edgePresent' = edgePresent,
    vertexPresent' = vertexPresent,
    relationshipPresent' = relationshipPresent,
    relationshipId' = relationshipId,
    relationshipSrc' = relationshipSrc,
    relationshipDst' = relationshipDst,
    createdRelationshipCount' = createdRelationshipCount,
    edgeMetadata' = edgeMetadata,
    duplicateVertexRejected' = true,
    ambiguousRelationshipRejected' = ambiguousRelationshipRejected,
    materializedRows' = materializedRows,
    cursorOffset' = cursorOffset,
    cursorOpen' = cursorOpen,
    lastResponse' = "rejected",
    lastAction' = "rejectConflictingDuplicateVertex",
  }

  // The client-service cursor is materialized: later graph writes cannot alter
  // its stored rows. Direct low-level page tokens are BFG-008 and intentionally
  // outside this cursor action until their contract is approved.
  action openMaterializedCursor: bool = all {
    not(cursorOpen),
    edgePresent' = edgePresent,
    vertexPresent' = vertexPresent,
    relationshipPresent' = relationshipPresent,
    relationshipId' = relationshipId,
    relationshipSrc' = relationshipSrc,
    relationshipDst' = relationshipDst,
    createdRelationshipCount' = createdRelationshipCount,
    edgeMetadata' = edgeMetadata,
    duplicateVertexRejected' = duplicateVertexRejected,
    ambiguousRelationshipRejected' = ambiguousRelationshipRejected,
    materializedRows' = 2,
    cursorOffset' = 0,
    cursorOpen' = true,
    lastResponse' = "cursor-open",
    lastAction' = "openMaterializedCursor",
  }

  action fetchMaterializedCursorPage: bool = all {
    cursorOpen,
    cursorOffset < materializedRows,
    edgePresent' = edgePresent,
    vertexPresent' = vertexPresent,
    relationshipPresent' = relationshipPresent,
    relationshipId' = relationshipId,
    relationshipSrc' = relationshipSrc,
    relationshipDst' = relationshipDst,
    createdRelationshipCount' = createdRelationshipCount,
    edgeMetadata' = edgeMetadata,
    duplicateVertexRejected' = duplicateVertexRejected,
    ambiguousRelationshipRejected' = ambiguousRelationshipRejected,
    materializedRows' = materializedRows,
    cursorOffset' = cursorOffset + 1,
    cursorOpen' = cursorOffset + 1 < materializedRows,
    lastResponse' = "row",
    lastAction' = "fetchMaterializedCursorPage",
  }

  // Property changes are one atomic metadata/index projection. They do not
  // create or remove the structural adjacency that owns the relationship.
  action setEdgeMetadataCommand: bool = all {
    edgePresent,
    edgeMetadata != "set",
    edgePresent' = edgePresent,
    vertexPresent' = vertexPresent,
    relationshipPresent' = relationshipPresent,
    relationshipId' = relationshipId,
    relationshipSrc' = relationshipSrc,
    relationshipDst' = relationshipDst,
    createdRelationshipCount' = createdRelationshipCount,
    edgeMetadata' = "set",
    duplicateVertexRejected' = duplicateVertexRejected,
    ambiguousRelationshipRejected' = ambiguousRelationshipRejected,
    materializedRows' = materializedRows,
    cursorOffset' = cursorOffset,
    cursorOpen' = cursorOpen,
    lastResponse' = "updated",
    lastAction' = "setEdgeMetadataCommand",
  }

  action removeEdgeMetadataCommand: bool = all {
    edgePresent,
    edgeMetadata == "set",
    edgePresent' = edgePresent,
    vertexPresent' = vertexPresent,
    relationshipPresent' = relationshipPresent,
    relationshipId' = relationshipId,
    relationshipSrc' = relationshipSrc,
    relationshipDst' = relationshipDst,
    createdRelationshipCount' = createdRelationshipCount,
    edgeMetadata' = "none",
    duplicateVertexRejected' = duplicateVertexRejected,
    ambiguousRelationshipRejected' = ambiguousRelationshipRejected,
    materializedRows' = materializedRows,
    cursorOffset' = cursorOffset,
    cursorOpen' = cursorOpen,
    lastResponse' = "updated",
    lastAction' = "removeEdgeMetadataCommand",
  }

  action step: bool = any {
    createEdgeCommand,
    createRelationshipCommand,
    mergeSameRelationshipCommand,
    rejectAmbiguousRelationshipId,
    rejectConflictingDuplicateVertex,
    setEdgeMetadataCommand,
    removeEdgeMetadataCommand,
    openMaterializedCursor,
    fetchMaterializedCursorPage,
  }

  val relationshipIdentityIsStable: bool =
    not(relationshipPresent) or all {
      relationshipId == 7,
      relationshipSrc == 1,
      relationshipDst == 2,
      createdRelationshipCount == 1,
    }
  val conflictingVertexBatchHasNoMutation: bool =
    not(duplicateVertexRejected) or vertexPresent
  val materializedCursorBounds: bool =
    cursorOffset >= 0 and cursorOffset <= materializedRows
  val cursorRowsRemainMaterialized: bool =
    not(cursorOpen) or materializedRows == 2
  val ambiguousRelationshipNeverAliases: bool =
    not(ambiguousRelationshipRejected) or all {
      relationshipSrc == 1,
      relationshipDst == 2,
      createdRelationshipCount == 1,
    }
  val metadataRequiresStructuralEdge: bool =
    edgeMetadata == "none" or edgePresent
  val allSafety: bool =
    relationshipIdentityIsStable and
      conflictingVertexBatchHasNoMutation and
      materializedCursorBounds and
      cursorRowsRemainMaterialized and
      ambiguousRelationshipNeverAliases and
      metadataRequiresStructuralEdge

  val ambiguousRelationshipRejectionReached: bool =
    lastAction == "rejectAmbiguousRelationshipId"
  val duplicateVertexRejectionReached: bool =
    lastAction == "rejectConflictingDuplicateVertex"
  val edgeCreateReached: bool = lastAction == "createEdgeCommand"
  val relationshipCreateReached: bool = lastAction == "createRelationshipCommand"
  val relationshipMergeReached: bool = lastAction == "mergeSameRelationshipCommand"
  val materializedCursorOpenedReached: bool = lastAction == "openMaterializedCursor"
  val materializedCursorPageFetchedReached: bool =
    lastAction == "fetchMaterializedCursorPage"
  val edgeMetadataSetReached: bool = lastAction == "setEdgeMetadataCommand"
  val edgeMetadataRemovedReached: bool = lastAction == "removeEdgeMetadataCommand"
}
