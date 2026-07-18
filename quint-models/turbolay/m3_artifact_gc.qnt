// -*- mode: quint -*-
//
// M3 — immutable artifact publication, topology generation, and retention.
//
// `canonicalReachable` is the source-of-truth traversal result. A builder
// copies it at one generation. Publication is legal only if no topology write
// advanced the dirty generation after that copy. The query action abstracts
// base-artifact plus delta overlay as one result that must equal canonical.
module m3_artifact_gc {
  var epoch: int
  var previousEpoch: int
  var canonicalReachable: bool
  var dirtyGeneration: int
  var artifactPublished: bool
  var artifactEpoch: int
  var artifactReachable: bool
  var builderActive: bool
  var builderBaseEpoch: int
  var builderReachable: bool
  var builderGeneration: int
  var stalePublishRejected: bool
  var readerActive: bool
  var readerEpoch: int
  var historyRetained: bool
  var queryResult: bool
  var lastAction: str

  action init: bool = all {
    epoch' = 0,
    previousEpoch' = 0,
    canonicalReachable' = false,
    dirtyGeneration' = 0,
    artifactPublished' = false,
    artifactEpoch' = 0,
    artifactReachable' = false,
    builderActive' = false,
    builderBaseEpoch' = 0,
    builderReachable' = false,
    builderGeneration' = 0,
    stalePublishRejected' = false,
    readerActive' = false,
    readerEpoch' = 0,
    historyRetained' = true,
    queryResult' = false,
    lastAction' = "init",
  }

  action writeCreate: bool = all {
    not(canonicalReachable),
    epoch' = epoch + 1,
    previousEpoch' = epoch,
    canonicalReachable' = true,
    dirtyGeneration' = epoch + 1,
    artifactPublished' = artifactPublished,
    artifactEpoch' = artifactEpoch,
    artifactReachable' = artifactReachable,
    builderActive' = builderActive,
    builderBaseEpoch' = builderBaseEpoch,
    builderReachable' = builderReachable,
    builderGeneration' = builderGeneration,
    stalePublishRejected' = stalePublishRejected,
    readerActive' = readerActive,
    readerEpoch' = readerEpoch,
    historyRetained' = true,
    queryResult' = queryResult,
    lastAction' = "writeCreate",
  }

  action writeDelete: bool = all {
    canonicalReachable,
    epoch' = epoch + 1,
    previousEpoch' = epoch,
    canonicalReachable' = false,
    dirtyGeneration' = epoch + 1,
    artifactPublished' = artifactPublished,
    artifactEpoch' = artifactEpoch,
    artifactReachable' = artifactReachable,
    builderActive' = builderActive,
    builderBaseEpoch' = builderBaseEpoch,
    builderReachable' = builderReachable,
    builderGeneration' = builderGeneration,
    stalePublishRejected' = stalePublishRejected,
    readerActive' = readerActive,
    readerEpoch' = readerEpoch,
    historyRetained' = true,
    queryResult' = queryResult,
    lastAction' = "writeDelete",
  }

  action startArtifactBuild: bool = all {
    not(builderActive),
    epoch' = epoch,
    previousEpoch' = epoch,
    canonicalReachable' = canonicalReachable,
    dirtyGeneration' = dirtyGeneration,
    artifactPublished' = artifactPublished,
    artifactEpoch' = artifactEpoch,
    artifactReachable' = artifactReachable,
    builderActive' = true,
    builderBaseEpoch' = epoch,
    builderReachable' = canonicalReachable,
    builderGeneration' = dirtyGeneration,
    stalePublishRejected' = stalePublishRejected,
    readerActive' = readerActive,
    readerEpoch' = readerEpoch,
    historyRetained' = historyRetained,
    queryResult' = queryResult,
    lastAction' = "startArtifactBuild",
  }

  action publishCurrentBuild: bool = all {
    builderActive,
    builderGeneration == dirtyGeneration,
    epoch' = epoch,
    previousEpoch' = epoch,
    canonicalReachable' = canonicalReachable,
    dirtyGeneration' = dirtyGeneration,
    artifactPublished' = true,
    artifactEpoch' = builderBaseEpoch,
    artifactReachable' = builderReachable,
    builderActive' = false,
    builderBaseEpoch' = builderBaseEpoch,
    builderReachable' = builderReachable,
    builderGeneration' = builderGeneration,
    stalePublishRejected' = stalePublishRejected,
    readerActive' = readerActive,
    readerEpoch' = readerEpoch,
    historyRetained' = historyRetained,
    queryResult' = queryResult,
    lastAction' = "publishCurrentBuild",
  }

  // A writer changed topology after this builder copied its source. It must
  // abort/retry; publishing it would be a stale artifact bug.
  action rejectStalePublish: bool = all {
    builderActive,
    builderGeneration < dirtyGeneration,
    epoch' = epoch,
    previousEpoch' = epoch,
    canonicalReachable' = canonicalReachable,
    dirtyGeneration' = dirtyGeneration,
    artifactPublished' = artifactPublished,
    artifactEpoch' = artifactEpoch,
    artifactReachable' = artifactReachable,
    builderActive' = false,
    builderBaseEpoch' = builderBaseEpoch,
    builderReachable' = builderReachable,
    builderGeneration' = builderGeneration,
    stalePublishRejected' = true,
    readerActive' = readerActive,
    readerEpoch' = readerEpoch,
    historyRetained' = historyRetained,
    queryResult' = queryResult,
    lastAction' = "rejectStalePublish",
  }

  action beginRead: bool = all {
    not(readerActive),
    epoch' = epoch,
    previousEpoch' = epoch,
    canonicalReachable' = canonicalReachable,
    dirtyGeneration' = dirtyGeneration,
    artifactPublished' = artifactPublished,
    artifactEpoch' = artifactEpoch,
    artifactReachable' = artifactReachable,
    builderActive' = builderActive,
    builderBaseEpoch' = builderBaseEpoch,
    builderReachable' = builderReachable,
    builderGeneration' = builderGeneration,
    stalePublishRejected' = stalePublishRejected,
    readerActive' = true,
    readerEpoch' = epoch,
    historyRetained' = true,
    queryResult' = queryResult,
    lastAction' = "beginRead",
  }

  action endRead: bool = all {
    readerActive,
    epoch' = epoch,
    previousEpoch' = epoch,
    canonicalReachable' = canonicalReachable,
    dirtyGeneration' = dirtyGeneration,
    artifactPublished' = artifactPublished,
    artifactEpoch' = artifactEpoch,
    artifactReachable' = artifactReachable,
    builderActive' = builderActive,
    builderBaseEpoch' = builderBaseEpoch,
    builderReachable' = builderReachable,
    builderGeneration' = builderGeneration,
    stalePublishRejected' = stalePublishRejected,
    readerActive' = false,
    readerEpoch' = readerEpoch,
    historyRetained' = historyRetained,
    queryResult' = queryResult,
    lastAction' = "endRead",
  }

  // Collection cannot remove history while a read has pinned it.
  action gcHistory: bool = all {
    not(readerActive),
    epoch' = epoch,
    previousEpoch' = epoch,
    canonicalReachable' = canonicalReachable,
    dirtyGeneration' = dirtyGeneration,
    artifactPublished' = artifactPublished,
    artifactEpoch' = artifactEpoch,
    artifactReachable' = artifactReachable,
    builderActive' = builderActive,
    builderBaseEpoch' = builderBaseEpoch,
    builderReachable' = builderReachable,
    builderGeneration' = builderGeneration,
    stalePublishRejected' = stalePublishRejected,
    readerActive' = readerActive,
    readerEpoch' = readerEpoch,
    historyRetained' = false,
    queryResult' = queryResult,
    lastAction' = "gcHistory",
  }

  // The implementation may use the artifact plus retained deltas or the
  // canonical path. In either case it must return the canonical answer.
  action queryMatrix: bool = all {
    not(readerActive) or historyRetained,
    epoch' = epoch,
    previousEpoch' = epoch,
    canonicalReachable' = canonicalReachable,
    dirtyGeneration' = dirtyGeneration,
    artifactPublished' = artifactPublished,
    artifactEpoch' = artifactEpoch,
    artifactReachable' = artifactReachable,
    builderActive' = builderActive,
    builderBaseEpoch' = builderBaseEpoch,
    builderReachable' = builderReachable,
    builderGeneration' = builderGeneration,
    stalePublishRejected' = stalePublishRejected,
    readerActive' = readerActive,
    readerEpoch' = readerEpoch,
    historyRetained' = historyRetained,
    queryResult' = canonicalReachable,
    lastAction' = "queryMatrix",
  }

  action step: bool = any {
    writeCreate,
    writeDelete,
    startArtifactBuild,
    publishCurrentBuild,
    rejectStalePublish,
    beginRead,
    endRead,
    gcHistory,
    queryMatrix,
  }

  val epochNeverRegresses: bool = epoch >= previousEpoch
  val publishedArtifactIsCurrentAtPublication: bool =
    not(lastAction == "publishCurrentBuild") or artifactEpoch == epoch
  // A stale candidate may be rejected and a fresh builder may start later.
  // The safety condition is therefore about the publication transition itself,
  // not a sticky historical rejection flag.
  val staleBuilderCannotPublish: bool =
    not(lastAction == "publishCurrentBuild") or builderGeneration == dirtyGeneration
  val activeReadRetainsHistory: bool = not(readerActive) or historyRetained
  val matrixEqualsCanonical: bool =
    not(lastAction == "queryMatrix") or queryResult == canonicalReachable
  val artifactGenerationNeverExceedsCurrent: bool =
    not(artifactPublished) or artifactEpoch <= epoch

  val stalePublishRejectedReached: bool = lastAction == "rejectStalePublish"
  val matrixQueryReached: bool = lastAction == "queryMatrix"
  val activeReadReached: bool = lastAction == "beginRead"
}
