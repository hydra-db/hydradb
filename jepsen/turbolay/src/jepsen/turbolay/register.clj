(ns jepsen.turbolay.register
  "Linearizable register over a vertex property.

   Each key k is an independent register held in `u.val` for the vertex with
   id k. Writes are `SET u.val = $v`; reads are `RETURN u.val`. Reads run with
   `consistency: strong`, which the docs define as 'observes every durable
   write committed before the refresh completed' — i.e. a linearizability
   claim. Knossos checks exactly that."
  (:require [clojure.tools.logging :refer [info]]
            [jepsen [client :as client]
                    [checker :as checker]
                    [generator :as gen]
                    [independent :as independent]]
            [knossos.model :as model]
            [jepsen.turbolay.http :as h]
            [jepsen.turbolay.routing :as routing]))

;; Anchor vertex every register key points at, so the key vertex exists.
(def anchor-id 999999999)

(defn r [_ _] {:type :invoke, :f :read,  :value nil})
(defn w [_ _] {:type :invoke, :f :write, :value (rand-int 5)})

(defrecord Client [node nodes consistency writer]
  client/Client
  (open! [this test node]
    (assoc this :node node :nodes (vec (:nodes test))))

  (setup! [this test])

  (invoke! [this test op]
    (let [[k v] (:value op)]
      (case (:f op)
        ;; Reads spread across all nodes — a strong read must be correct
        ;; wherever it lands, including on a node that never wrote.
        :read
        (let [target (rand-nth nodes)
              op     (assoc op :node target)]
          (h/with-errors op
            (let [res (h/query! target
                                "MATCH (u {id: $k}) RETURN u.val AS v"
                                {:params      {:k k}
                                 :consistency consistency
                                 :timeout     20000})
                  val (some-> (first (:rows res)) first)]
              (assoc op :type :ok :value (independent/tuple k val)))))

        :write
        (let [target (routing/write-node writer)
              op     (assoc op :node target)
              res    (h/with-errors op
                       ;; MERGE materialises the key vertex on first touch, so
                       ;; there is no setup phase racing the nemesis.
                       (do
                         (h/query! target
                                   (str "MERGE (u {id: $k})-[:ANCHOR]->(a {id: "
                                        anchor-id "})")
                                   {:params {:k k} :timeout 15000})
                         (h/query! target
                                   "MATCH (u {id: $k}) SET u.val = $v"
                                   {:params {:k k :v v} :timeout 15000})
                         (assoc op :type :ok)))]
          (when (not= :ok (:type res)) (routing/advance! writer))
          res))))

  (teardown! [this test])
  (close! [this test]))

(defn workload
  [opts]
  {:client  (map->Client {:consistency (:consistency opts "strong")
                          :writer      (routing/writer-state (:nodes opts))})
   :checker (independent/checker
              (checker/linearizable {:model     (model/cas-register)
                                     :algorithm :linear}))
   :generator (independent/concurrent-generator
                (:concurrency-per-key opts 5)
                (range)
                (fn [k]
                  (->> (gen/mix [r w])
                       (gen/stagger 1/10)
                       (gen/limit 120))))})
