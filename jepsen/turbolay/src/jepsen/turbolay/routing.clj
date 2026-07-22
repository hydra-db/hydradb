(ns jepsen.turbolay.routing
  "Writer affinity with failover.

   Turbolay allows any node to accept a write, but only one SlateDB writer can
   be live per graph store; a second node opening the writer fences the first.
   Fanning writes across all five nodes therefore makes them fence each other
   continuously — an early run of this suite produced 80 `l0_manifest_writer
   error=Fenced` events and 500s on two thirds of writes, with fencing itself
   working perfectly (zero lost writes).

   That is a client-behaviour problem, not a safety problem, and testing it
   forever would only re-measure thrash. Real clients follow the Bolt routing
   table to the current writer, so this namespace models the same thing: all
   writes go to one node, and on failure the client advances to the next node.

   That is also what makes the fault injection meaningful — killing the node
   that currently holds writer affinity is exactly the writer-handover event
   the fencing logic exists to make safe."
  (:require [clojure.tools.logging :refer [info]]))

(defn writer-state
  "Shared, mutable writer affinity for one test run."
  [nodes]
  (atom {:nodes (vec nodes), :idx 0}))

(defn write-node
  [state]
  (let [{:keys [nodes idx]} @state]
    (nth nodes (mod idx (count nodes)))))

(defn advance!
  "Called after a failed write: move affinity to the next node, the way a
   routing-aware driver would on a NOT_THE_LEADER-style error."
  [state]
  (swap! state update :idx inc)
  (write-node state))
