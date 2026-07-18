// -*- mode: quint -*-
module m3_artifact_gc_test {
  import m3_artifact_gc.* from "./m3_artifact_gc"

  run staleBuilderCannotPublishAfterTopologyChangeTest =
    init
      .then(writeCreate)
      .then(startArtifactBuild)
      .then(writeDelete)
      .then(rejectStalePublish)
      .expect(stalePublishRejected)
      .expect(not(artifactPublished))
      .expect(not(canonicalReachable))

  run freshArtifactAndDeltaQueryMatchCanonicalTest =
    init
      .then(writeCreate)
      .then(startArtifactBuild)
      .then(publishCurrentBuild)
      .then(queryMatrix)
      .expect(artifactPublished)
      .expect(queryResult == canonicalReachable)

  run activeReadBlocksHistoryCollectionTest =
    init
      .then(beginRead)
      .expect(historyRetained)
      .expect(readerActive)
      .then(queryMatrix)
      .expect(queryResult == canonicalReachable)
}
