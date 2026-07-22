// -*- mode: quint -*-
module m5_destructive_lifecycle_test {
  import m5_destructive_lifecycle.* from "./m5_destructive_lifecycle"

  run deleteRequiresDetachWhenEdgesAreIncidentTest =
    init
      .then(rejectDeleteWithIncidentEdges)
      .expect(deleteRejected)
      .expect(vertexPresent)
      .expect(incidentEdgeCount == 2)

  run detachDeleteRemovesVertexAndAllIncidentEdgesTest =
    init
      .then(detachDeleteVertex)
      .expect(vertexDeleted)
      .expect(not(vertexPresent))
      .expect(incidentEdgeCount == 0)

  run dropCellFencesFutureWritesTest =
    init
      .then(dropCell)
      .expect(cellDropped)
      .expect(not(vertexPresent))
      .then(rejectWriteAfterDrop)
      .expect(postDropWriteRejected)
      .expect(incidentEdgeCount == 0)
}
