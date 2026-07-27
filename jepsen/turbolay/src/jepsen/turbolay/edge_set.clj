(ns jepsen.turbolay.edge-set
  "Grow-only edge set.

   Writers CREATE a distinct outbound edge (root)-[:E]->(id: v); readers scan
   the full outbound adjacency of the root. Because the set only ever grows,
   any acknowledged element that a later read fails to return is a lost write
   or an illegal stale read — this is the workload that puts pressure on
   writer fencing, WAL-tail-plus-index merging, and the strong-read refresh.

   Writes follow writer affinity (see jepsen.turbolay.routing); reads are
   spread across every node, since a read served by a node that never accepted
   a write is precisely the case where a stale view would show up."
  (:require [clojure.tools.logging :refer [info]]
            [jepsen [client :as client]
                    [checker :as checker]
                    [generator :as gen]]
            [jepsen.turbolay.http :as h]
            [jepsen.turbolay.routing :as routing]))

(def root-id 1)
(def edge-type "E")

;; Client timeouts must stay at or below GRAPH_MAX_QUERY_RUNTIME_MS; the
;; service rejects an over-cap request with 429 rather than clamping it.
(def write-timeout 15000)
(def read-timeout  30000)

(defrecord Client [node nodes consistency writer]
  client/Client
  (open! [this test node]
    (assoc this :node node :nodes (vec (:nodes test))))

  (setup! [this test])

  (invoke! [this test op]
    (case (:f op)
      :add
      (let [target (routing/write-node writer)
            op     (assoc op :node target)
            res    (h/with-errors op
                     (let [r (h/query! target
                                       (str "CREATE (a {id: " root-id "})-[:" edge-type
                                            "]->(b {id: $v})")
                                       {:params  {:v (:value op)}
                                        :timeout write-timeout})]
                       (assoc op :type :ok :bookmark (:bookmark r))))]
        ;; Any non-:ok write means this node is no longer a good writer —
        ;; it was fenced, killed, or cut off from S3. Move affinity on.
        (when (not= :ok (:type res))
          (routing/advance! writer))
        res)

      :read
      ;; Reads deliberately land anywhere, including nodes that have never
      ;; held the writer.
      (let [target (rand-nth nodes)
            op     (assoc op :node target)]
        (h/with-errors op
          (let [r (h/query! target
                            (str "MATCH (a {id: " root-id "})-[:" edge-type "]->(b) "
                                 "RETURN b.id AS v")
                            {:consistency consistency
                             :page-size   1000
                             :timeout     read-timeout})]
            (assoc op
                   :type  :ok
                   :value (into (sorted-set) (map first (:rows r)))
                   :bookmark (:bookmark r)))))))

  (teardown! [this test])
  (close! [this test]))

(defn workload
  [opts]
  {:client  (map->Client {:consistency (:consistency opts "strong")
                          :writer      (routing/writer-state (:nodes opts))})
   :checker (checker/set-full {:linearizable? false})
   ;; Adds are a lazy seq of distinct values; reads are interleaved so we
   ;; catch transient stale views rather than only the final state.
   :generator
   (->> (gen/mix [(map (fn [v] {:f :add, :value v}) (range))
                  (repeat {:f :read})])
        (gen/stagger 1/20))
   ;; After healing, every thread reads once. Anything missing here is lost.
   :final-generator (gen/each-thread {:f :read})})
