// -*- mode: quint -*-
//
// M4 — local placement views versus the authoritative durable writer fence.
// Membership disagreement can produce competing candidates, but only a fence
// acquisition can create an effective writer and commit graph state.
module m4_placement_fence {
  var durableFence: int
  var previousFence: int
  var node1Candidate: bool
  var node2Candidate: bool
  var node1Active: bool
  var node2Active: bool
  var node1Reachable: bool
  var node2Reachable: bool
  var committedPrefix: int
  var previousPrefix: int
  var lastAction: str

  action init: bool = all {
    durableFence' = 0,
    previousFence' = 0,
    node1Candidate' = false,
    node2Candidate' = false,
    node1Active' = false,
    node2Active' = false,
    node1Reachable' = true,
    node2Reachable' = true,
    committedPrefix' = 0,
    previousPrefix' = 0,
    lastAction' = "init",
  }

  action chooseNode1: bool = all {
    durableFence' = durableFence,
    previousFence' = durableFence,
    node1Candidate' = true,
    node2Candidate' = node2Candidate,
    node1Active' = node1Active,
    node2Active' = node2Active,
    node1Reachable' = node1Reachable,
    node2Reachable' = node2Reachable,
    committedPrefix' = committedPrefix,
    previousPrefix' = committedPrefix,
    lastAction' = "chooseNode1",
  }

  action chooseNode2: bool = all {
    durableFence' = durableFence,
    previousFence' = durableFence,
    node1Candidate' = node1Candidate,
    node2Candidate' = true,
    node1Active' = node1Active,
    node2Active' = node2Active,
    node1Reachable' = node1Reachable,
    node2Reachable' = node2Reachable,
    committedPrefix' = committedPrefix,
    previousPrefix' = committedPrefix,
    lastAction' = "chooseNode2",
  }

  action acquireFenceNode1: bool = all {
    node1Candidate,
    not(node2Active),
    durableFence' = 1,
    previousFence' = durableFence,
    node1Candidate' = node1Candidate,
    node2Candidate' = node2Candidate,
    node1Active' = true,
    node2Active' = false,
    node1Reachable' = node1Reachable,
    node2Reachable' = node2Reachable,
    committedPrefix' = committedPrefix,
    previousPrefix' = committedPrefix,
    lastAction' = "acquireFenceNode1",
  }

  action commitNode1: bool = all {
    node1Active,
    node1Reachable,
    durableFence == 1,
    durableFence' = durableFence,
    previousFence' = durableFence,
    node1Candidate' = node1Candidate,
    node2Candidate' = node2Candidate,
    node1Active' = node1Active,
    node2Active' = node2Active,
    node1Reachable' = node1Reachable,
    node2Reachable' = node2Reachable,
    committedPrefix' = committedPrefix + 1,
    previousPrefix' = committedPrefix,
    lastAction' = "commitNode1",
  }

  // A partition affects local knowledge and availability, not the persisted
  // fence. A later takeover atomically fences this old writer before enabling
  // its replacement.
  action partitionNode1: bool = all {
    durableFence' = durableFence,
    previousFence' = durableFence,
    node1Candidate' = node1Candidate,
    node2Candidate' = node2Candidate,
    node1Active' = node1Active,
    node2Active' = node2Active,
    node1Reachable' = false,
    node2Reachable' = node2Reachable,
    committedPrefix' = committedPrefix,
    previousPrefix' = committedPrefix,
    lastAction' = "partitionNode1",
  }

  action takeOverFenceNode2: bool = all {
    node2Candidate,
    not(node1Reachable),
    durableFence' = 2,
    previousFence' = durableFence,
    node1Candidate' = node1Candidate,
    node2Candidate' = node2Candidate,
    node1Active' = false,
    node2Active' = true,
    node1Reachable' = node1Reachable,
    node2Reachable' = node2Reachable,
    committedPrefix' = committedPrefix,
    previousPrefix' = committedPrefix,
    lastAction' = "takeOverFenceNode2",
  }

  action commitNode2: bool = all {
    node2Active,
    node2Reachable,
    durableFence == 2,
    durableFence' = durableFence,
    previousFence' = durableFence,
    node1Candidate' = node1Candidate,
    node2Candidate' = node2Candidate,
    node1Active' = node1Active,
    node2Active' = node2Active,
    node1Reachable' = node1Reachable,
    node2Reachable' = node2Reachable,
    committedPrefix' = committedPrefix + 1,
    previousPrefix' = committedPrefix,
    lastAction' = "commitNode2",
  }

  action rejectZombieNode1Commit: bool = all {
    durableFence == 2,
    not(node1Active),
    durableFence' = durableFence,
    previousFence' = durableFence,
    node1Candidate' = node1Candidate,
    node2Candidate' = node2Candidate,
    node1Active' = node1Active,
    node2Active' = node2Active,
    node1Reachable' = node1Reachable,
    node2Reachable' = node2Reachable,
    committedPrefix' = committedPrefix,
    previousPrefix' = committedPrefix,
    lastAction' = "rejectZombieNode1Commit",
  }

  action step: bool = any {
    chooseNode1,
    chooseNode2,
    acquireFenceNode1,
    commitNode1,
    partitionNode1,
    takeOverFenceNode2,
    commitNode2,
    rejectZombieNode1Commit,
  }

  val atMostOneEffectiveWriter: bool = not(node1Active and node2Active)
  val durableFenceMonotone: bool = durableFence >= previousFence
  val committedPrefixNeverRegresses: bool = committedPrefix >= previousPrefix
  val activeWriterMatchesFence: bool =
    ((durableFence == 1) implies not(node2Active)) and
      ((durableFence == 2) implies not(node1Active))
  val staleWriterCannotCommit: bool = not(lastAction == "commitNode1" and durableFence == 2)
  val allSafety: bool =
    atMostOneEffectiveWriter and
      durableFenceMonotone and
      committedPrefixNeverRegresses and
      activeWriterMatchesFence and
      staleWriterCannotCommit

  val candidateDisagreementReached: bool = node1Candidate and node2Candidate
  val takeoverReached: bool = lastAction == "takeOverFenceNode2"
  val zombieRejectionReached: bool = lastAction == "rejectZombieNode1Commit"
  val node1ChosenReached: bool = lastAction == "chooseNode1"
  val node2ChosenReached: bool = lastAction == "chooseNode2"
  val node1FenceAcquiredReached: bool = lastAction == "acquireFenceNode1"
  val node1CommitReached: bool = lastAction == "commitNode1"
  val node1PartitionedReached: bool = lastAction == "partitionNode1"
  val node2CommitReached: bool = lastAction == "commitNode2"
}
