// -*- mode: quint -*-
module m4_placement_fence_test {
  import m4_placement_fence.* from "./m4_placement_fence"

  run disagreementDoesNotCreateTwoWritersTest =
    init
      .then(chooseNode1)
      .then(chooseNode2)
      .expect(node1Candidate and node2Candidate)
      .then(acquireFenceNode1)
      .expect(node1Active)
      .expect(not(node2Active))

  run takeoverFencesOldWriterTest =
    init
      .then(chooseNode1)
      .then(chooseNode2)
      .then(acquireFenceNode1)
      .then(commitNode1)
      .then(partitionNode1)
      .then(takeOverFenceNode2)
      .then(rejectZombieNode1Commit)
      .expect(durableFence == 2)
      .expect(node2Active)
      .expect(not(node1Active))
      .expect(committedPrefix == 1)

  run replacementExtendsCommittedPrefixTest =
    init
      .then(chooseNode1)
      .then(chooseNode2)
      .then(acquireFenceNode1)
      .then(commitNode1)
      .then(partitionNode1)
      .then(takeOverFenceNode2)
      .then(commitNode2)
      .expect(committedPrefix == 2)
      .expect(durableFence == 2)
}
