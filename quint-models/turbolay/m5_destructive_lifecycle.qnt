// -*- mode: quint -*-
//
// M5b — DELETE, DETACH DELETE, and cell-drop contract.
//
// A plain vertex delete is rejected while incident edges exist. DETACH DELETE
// removes the vertex and every incident edge as one public operation. Dropping
// a cell leaves a durable fence: no future graph write may recreate state in
// that namespace.
module m5_destructive_lifecycle {
  var vertexPresent: bool
  var incidentEdgeCount: int
  var vertexDeleted: bool
  var cellDropped: bool
  var deleteRejected: bool
  var postDropWriteRejected: bool
  var lastAction: str

  action init: bool = all {
    vertexPresent' = true,
    incidentEdgeCount' = 2,
    vertexDeleted' = false,
    cellDropped' = false,
    deleteRejected' = false,
    postDropWriteRejected' = false,
    lastAction' = "init",
  }

  action rejectDeleteWithIncidentEdges: bool = all {
    vertexPresent,
    incidentEdgeCount > 0,
    not(cellDropped),
    vertexPresent' = vertexPresent,
    incidentEdgeCount' = incidentEdgeCount,
    vertexDeleted' = vertexDeleted,
    cellDropped' = cellDropped,
    deleteRejected' = true,
    postDropWriteRejected' = postDropWriteRejected,
    lastAction' = "rejectDeleteWithIncidentEdges",
  }

  action detachDeleteVertex: bool = all {
    vertexPresent,
    not(cellDropped),
    vertexPresent' = false,
    incidentEdgeCount' = 0,
    vertexDeleted' = true,
    cellDropped' = false,
    deleteRejected' = false,
    postDropWriteRejected' = false,
    lastAction' = "detachDeleteVertex",
  }

  action dropCell: bool = all {
    not(cellDropped),
    vertexPresent' = false,
    incidentEdgeCount' = 0,
    vertexDeleted' = vertexDeleted,
    cellDropped' = true,
    deleteRejected' = false,
    postDropWriteRejected' = false,
    lastAction' = "dropCell",
  }

  action rejectWriteAfterDrop: bool = all {
    cellDropped,
    vertexPresent' = vertexPresent,
    incidentEdgeCount' = incidentEdgeCount,
    vertexDeleted' = vertexDeleted,
    cellDropped' = cellDropped,
    deleteRejected' = deleteRejected,
    postDropWriteRejected' = true,
    lastAction' = "rejectWriteAfterDrop",
  }

  action step: bool = any {
    rejectDeleteWithIncidentEdges,
    detachDeleteVertex,
    dropCell,
    rejectWriteAfterDrop,
  }

  val rejectedDeleteDidNotMutate: bool =
    lastAction != "rejectDeleteWithIncidentEdges" or all {
      vertexPresent,
      incidentEdgeCount > 0,
      not(vertexDeleted),
    }
  val detachDeleteRemovesAllIncidentState: bool =
    not(vertexDeleted) or all {
      not(vertexPresent),
      incidentEdgeCount == 0,
    }
  val droppedCellContainsNoGraphState: bool =
    not(cellDropped) or all {
      not(vertexPresent),
      incidentEdgeCount == 0,
    }
  val postDropWriteCannotRestoreState: bool =
    not(postDropWriteRejected) or all {
      cellDropped,
      not(vertexPresent),
      incidentEdgeCount == 0,
    }
  val allSafety: bool =
    rejectedDeleteDidNotMutate and
      detachDeleteRemovesAllIncidentState and
      droppedCellContainsNoGraphState and
      postDropWriteCannotRestoreState
}
